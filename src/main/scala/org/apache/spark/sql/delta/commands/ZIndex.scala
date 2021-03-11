/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.commands

import java.util.UUID

import scala.collection.mutable.HashMap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

trait ZIndex[T] {

  def indices: T

  def toBinaryString: String

}

case class ArrayZIndex(override val indices: Array[Int]) extends ZIndex[Array[Int]] {
  override def toBinaryString: String = {
    indices.mkString("")
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

  // scalastyle:off line.size.limit
  /**
   * reference: https://medium.com/swlh/computing-global-rank-of-a-row-in-a-dataframe-with-spark-sql-34f6cc650ae5
   */
  // scalastyle:on line.size.limit
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
      .orderBy(colName)
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

    // todo:(fchen) return long type rankCol, so that we have max Long.MaxValue table size.
    val finalDF = rankDF.join(
      broadcast(joinDF), Seq("partitionId"), "inner")
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
