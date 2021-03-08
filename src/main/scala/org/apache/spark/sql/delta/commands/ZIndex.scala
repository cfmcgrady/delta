// scalastyle:off
// todo:(fchen) fix style
package org.apache.spark.sql.delta.commands

import java.util.UUID

import scala.collection.mutable.HashMap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

/**
 * @time 2021/3/7 5:31 下午
 * @author fchen <cloud.chenfu@gmail.com>
 */
trait ZIndex[T] {

  def indices: T

  def toBinaryString: String

  /**
   * @return 1 means this > that, 0 means this = that, -1 means this < that.
   */
  def compare(that: ZIndex[T]): Int

  def >=(that: ZIndex[T]): Boolean = {
    val f = compare(that)
    f == 1 || f == 0
  }

  def >(that: ZIndex[T]): Boolean = {
    val f = compare(that)
    f == 1
  }

  def <=(that: ZIndex[T]): Boolean = {
    val f = compare(that)
    f == -1 || f == 0
  }

  def <(that: ZIndex[T]): Boolean = {
    val f = compare(that)
    f == -1
  }
}

case class ArrayZIndex(override val indices: Array[Int]) extends ZIndex[Array[Int]] {
  override def toBinaryString: String = {
    indices.mkString("")
  }

  /**
   * @return 1 means this > that, 0 means this = that, -1 means this < that.
   */
  override def compare(that: ZIndex[Array[Int]]): Int = {
    require(this.indices.length == that.indices.length)
    for (i <- (0 until indices.length)) {
      val diff = indices(i) - that.indices(i)
      if (diff != 0) {
        return diff
      }
    }
    return 0
  }
}

object ArrayZIndex {
  def create(input: Array[Int]): ArrayZIndex = {
    val zindex = (31 to 0 by -1).flatMap(i => {
      val mask = 1 << i
      (0 until input.length).map( colIndex => {
        if ((input(colIndex) & mask) != 0) {
          1
        } else {
          0
        }
      })
    })
    ArrayZIndex(zindex.toArray)
  }

  def create(input: Seq[Int]): ArrayZIndex = {
    create(input.toArray)
  }
}

object ZIndexUtil {

  private val colToIndexColName = HashMap[String, String]()

  private val indexColName = {
    (colName: String) =>
      colToIndexColName.getOrElseUpdate(
        colName,
        s"__${UUID.randomUUID().toString.replace("-", "")}__"
      )
  }
  private val minColName = {
    (colName: String) => s"__min_${indexColName(colName)}__"
  }

  private val maxColName = {
    (colName: String) => s"__max_${indexColName(colName)}__"
  }

  private val countColName = {
    (colName: String) => s"__count_${indexColName(colName)}__"
  }

  def createZIndex(
      inputDF: DataFrame,
      zorderBy: Seq[String],
      outputFileNum: Int = 1000): DataFrame = {
    inputDF.sparkSession
      .udf
      .register("arrayZIndex", (vec: Seq[Int]) => ArrayZIndex.create(vec).indices)

    val dataSchema = inputDF.schema.map(_.name)

    var rowIdDF: DataFrame = inputDF

    zorderBy.foreach(c => {
      // TODO:(fchen) read 10000000 from configurations.
      if (inputDF.selectExpr(c).distinct().count() < 10000000) {
        rowIdDF = generateRankIDForSkewedTable(rowIdDF, c, indexColName(c))
      } else {
        rowIdDF = generateGlobalRankId(rowIdDF, c, indexColName(c))
      }
      //        rowIdDF = generateGlobalRankId(rowIdDF, c, indexColName(c))
      //        rowIdDF = generateRankIDForSkewedTable(rowIdDF, c, indexColName(c))
    })

    val indexDF = rowIdDF.selectExpr(
      (dataSchema ++ Array(
          s"arrayZIndex(array(${zorderBy.map(indexColName).mkString(",")})) as __zIndex__")): _*
    )

    indexDF
      .repartitionByRange(outputFileNum, col("__zIndex__"), (rand() * 1000).cast(IntegerType))
      .selectExpr(dataSchema: _*)
  }

  /**
   * reference: https://medium.com/swlh/computing-global-rank-of-a-row-in-a-dataframe-with-spark-sql-34f6cc650ae5
   *
   * @param df
   * @param colName
   * @param rankColName
   * @return
   */
  def generateGlobalRankId(df: DataFrame, colName: String, rankColName: String): DataFrame = {

    // if the input column is a nested column, we should normalize the input column name.
    if (isNestedColumn(df, colName)) {
      val newColName = s"__col_${colName.hashCode.toString.replaceAll("-", "")}__"
      val newDF = df.selectExpr("*", s"$colName as $newColName")
      return generateGlobalRankId(newDF, newColName, rankColName)
        .selectExpr((Seq(rankColName) ++ df.schema.map(_.name)): _*)
    }

    val inputSchema = df.schema.map(_.name)
    val spark = df.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val partDF = df
      .orderBy(colName) // ==> 通过range partition来实现的
      // =====> 在这里实现bin packing 的分区方法？
      .withColumn("partitionId", spark_partition_id())

    //    partDF.createOrReplaceTempView("aaa")
    //    spark.sql("select distinct(partitionId) as pid from aaa").show()

    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy("partitionId").orderBy(colName)

    //    val rankDF = partDF.withColumn("local_rank", dense_rank().over(w))
    val rankDF = partDF.withColumn("local_rank", row_number().over(w))
      .withColumn("rand_id", rand())
      .repartition(1000, col("rand_id"))
      .persist(StorageLevel.DISK_ONLY_2)

    rankDF.count

    val tempDf =
      rankDF.groupBy("partitionId").agg(max("local_rank").alias("max_rank"))

    val w2 = Window.orderBy("partitionId").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val statsDf = tempDf.withColumn("cum_rank", sum("max_rank").over(w2))

    val joinDF = statsDf.alias("l")
      .join(
        statsDf.alias("r"), $"l.partitionId" === $"r.partitionId" +1, "left"
      ).select(
      col("l.partitionId"),
      coalesce(col("r.cum_rank"), lit(0)).alias("sum_factor")
    )

    // todo: (fchen) 转为long类型
    val finalDF = rankDF.join(
      broadcast(joinDF), Seq("partitionId"),"inner")
      .withColumn(rankColName, ($"local_rank" + $"sum_factor" - 1).cast(IntegerType))

    rankDF.unpersist()

    finalDF.selectExpr((rankColName :: Nil ++ inputSchema): _*)
  }

  def generateRankIDForSkewedTable(df: DataFrame,
                                   colName: String,
                                   rankColName: String): DataFrame = {

    // if the input column is a nested column, we should normalize the input column name.
    if (isNestedColumn(df, colName)) {
      val newColName = s"__col_${colName.hashCode.toString.replaceAll("-", "")}__"
      val newDF = df.selectExpr("*", s"$colName as $newColName")
      return generateRankIDForSkewedTable(newDF, newColName, rankColName)
        .selectExpr((Seq(rankColName) ++ df.schema.map(_.name)): _*)
    }

    val inputSchema = df.schema.map(_.name)
    val spark = df.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window

    //    df.withColumn("rand_id", (rand() * 1000).cast(IntegerType))
    val cachedDF = df.persist(StorageLevel.DISK_ONLY_2)

    try {
      val idDF = cachedDF.dropDuplicates(colName)
        .withColumn(rankColName, rank.over(Window.orderBy(colName)) - 1)

      cachedDF.alias("l")
        .join(
          broadcast(idDF.alias("r")),
          $"l.${colName}" === $"r.${colName}",
          "left"
        )
        .selectExpr(inputSchema.map(i => s"l.$i") ++ Array(rankColName): _*)
    } finally {
      cachedDF.unpersist()
    }
  }

  private def isNestedColumn(df: DataFrame, colName: String): Boolean = {
    !df.schema.fields.map(_.name).contains(colName)
  }
}
