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

// scalstyle:off
// TODO:(fchen) fix style
package org.apache.spark.sql.delta

import java.io.File
import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame, QueryTest}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.util.Utils
import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfterEach

/**
 * @time 2021/3/5 2:35 下午
 * @author fchen <cloud.chenfu@gmail.com>
 */
class OptimizeSuiteBase
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach
    with SQLTestUtils {

  var tempDir: File = _

  var deltaLog: DeltaLog = _

  protected def tempPath = tempDir.getCanonicalPath

  protected def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  override def beforeEach() {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
  }

  override def afterEach() {
    try {
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  protected def executeUpdate(target: String, set: Seq[String], where: String): Unit = {
    executeUpdate(target, set.mkString(", "), where)
  }

  protected def append(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("delta").mode("append")
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    }
    writer.save(deltaLog.dataPath.toString)
  }

  implicit def jsonStringToSeq(json: String): Seq[String] = json.split("\n")

  val fileFormat: String = "parquet"

  test("aa") {
    val sparkSession = spark
    import sparkSession.implicits._

    println("hello")
    val df = (1 to 100).toSeq.toDF("id")
    append(df)
    println(deltaLog.dataPath.toString)
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(Seq("id"), 16)
    println("end")
    val df2 = table.toDF.filter("id = 1")
    df2.show
    df2.collect()
    println(table.toDF.count)


    df2.queryExecution.executedPlan.collectLeaves().foreach(l => {
      l.metrics.foreach {
        case (key, metric) if (key == "numFiles") =>
          println("numFile" + metric.value)
        case _ =>
      }
    })
//    Thread.sleep(Int.MaxValue)
  }

//  test("optimize partitioned table basic") {
  test("cc") {
    val sparkSession = spark
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val input = (1 to 100).map {
      case i if i % 2 == 0 => null
      case i => i.toString
    }.toDF("col_1")

    val df = Seq((1, "a"), (2, "b")).toDF("col_1", "pid")
    df.show
    append(df, Seq("pid"))
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(expr("pid = 'a'"), Seq("col_1"), 16)
//    Thread.sleep(Int.MaxValue)
//    table.toDF.filter("col_1 = 1").show()
    val df2 = table.toDF.filter("col_1 = 2")
    df2.show
    df2.collect()
    df2.queryExecution.executedPlan.collectLeaves().foreach(l => {
      l.metrics.foreach {
        case (key, metric) if (key == "numFiles") =>
          println("numFile" + metric.value)
        case _ =>
      }
    })
  }

  test("bb") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df1 = Seq(("a", 1), ("b", 2)).toDF("ca1", "cb")
    val df2 = Seq(("a", 1), ("b", 2)).toDF("ca2", "cc")
    df1.alias("l").join(
      df2.alias("r"),
      $"l.ca1" === $"r.ca2"
    ).selectExpr("r.*")
      .show


  }

  test("zorder by integer type basic case.") {
    val dataLength = 8
    spark.udf.register("vec", (i: Int) => (0 until i).toArray)
    val input = spark.range(0, dataLength)
      .selectExpr("id as col_1", s"explode(vec(${dataLength})) as col_2")
    append(input)
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(Seq("col_1", "col_2"), 16)

    val conditions = Seq(
      FilterTestInfo("col_1 = 2 or col_2 = 2", 7),
      FilterTestInfo("col_1 == 2 and col_2 == 2", 1),
      FilterTestInfo("col_1 == 2", 4),
      FilterTestInfo("col_2 == 2", 4),
      FilterTestInfo("1 == 1", 16),
      FilterTestInfo("1 == 2", None),
      FilterTestInfo("1 == 1 and col_1 == 2", 4),
      FilterTestInfo("1 == 2 and col_1 == 2", None),
      FilterTestInfo("1 == 1 or col_1 == 2", 16),
      FilterTestInfo("1 == 2 or col_1 == 2", 4),
      FilterTestInfo("1 == 1 and 2 == 2", 16),
      FilterTestInfo("col_1 < 2", 4),
      FilterTestInfo("col_1 > 5", 4),
      FilterTestInfo("col_1 < 3", 8),
      FilterTestInfo("col_1 <= 2", 8),
      FilterTestInfo("col_1 >= 5", 8),
      FilterTestInfo("col_1 <= 3", 8)
    )

    (conditions.map(c => c.copy(condition = exchangeLeftRight(c.condition))) ++ conditions)
      .foreach {
        case cond =>
          val res = table.toDF.filter(cond.condition)
          if (cond.numFiles.isDefined) {
            res.collect()
            val numFilesMetric = res.queryExecution
              .executedPlan
              .collectLeaves()
              .flatMap(_.metrics)
              .collect {
                case m @ (key, metric) if (key == "numFiles" && metric.value == cond.numFiles.get) =>
                  m
              }
            println("check condition: " + cond.condition)
            assert(numFilesMetric.size == 1)
          }
          checkAnswer(res, input.filter(cond.condition).collect())
      }
  }

  test("dd") {
    val sparkSession = spark
    import sparkSession.implicits._
    val input = (1 to 100).map {
      case i if i % 2 == 0 => null
      case i => i.toString
    }.toDF("col_1")
    append(input)
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(Seq("col_1"), 20)

    runZorderByFilterTest(
      table,
      input,
      Seq(
        FilterTestInfo("col_1 = '1'", 1),
        FilterTestInfo("col_1 = null", None),
        FilterTestInfo("col_1 < '3'", 3),
        FilterTestInfo("col_1 > '8'", 3),
        FilterTestInfo("col_1 is null", 10),
        FilterTestInfo("col_1 is not null", 10),
        FilterTestInfo("col_1 in ('1', '9')", 2),
        FilterTestInfo("col_1 in (null)", None),
        FilterTestInfo("col_1 not in ('1', '9')", 10),
        FilterTestInfo("col_1 not in ('1', '9', null)", 0),
        FilterTestInfo("col_1 in (select null)", None),
        FilterTestInfo("col_1 not in (select null)", None),
        FilterTestInfo("col_1 in ('1', null)", 1),
        FilterTestInfo("col_1 like '1%'", 2),
        FilterTestInfo("col_1 <=> null", 10),
        FilterTestInfo("col_1 <=> '1'", 1)
      )
    )
  }

  test("file filter with not in condition") {
    val sparkSession = spark
    import sparkSession.implicits._
    val input = Seq("a", "b", "c").toDF("col_1")
    append(input)
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(Seq("col_1"), 3)
    table.toDF.filter("'a' <> col_1").show()
    runZorderByFilterTest(
      table,
      input,
      Seq(
        FilterTestInfo("col_1 not in ('a', 'b')", 1),
        FilterTestInfo("col_1 <> 'a'", 2)
      )
    )
  }

  test("ee") {
    val sparkSession = spark
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val input = Seq(("a", "pa"), ("b", "pb"), ("c", "pc")).toDF("col_1", "pid")
    append(input, Seq("pid"))
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(expr("pid = 'pb'"), Seq("col_1"), 3)
    table.toDF.filter("col_1 <> 'a'").show()
    table.toDF.filter("col_1 <> 'a'").explain(true)
  }

  private def runZorderByFilterTest(table: DeltaTable,
                                    input: DataFrame,
                                    conditions: Seq[FilterTestInfo]): Unit = {
    (conditions.map(c => c.copy(condition = exchangeLeftRight(c.condition))) ++ conditions)
      .foreach {
        case cond =>
          val res = table.toDF.filter(cond.condition)
          if (cond.numFiles.isDefined) {
            res.collect()
            val numFilesMetric = res.queryExecution
              .executedPlan
              .collectLeaves()
              .flatMap(_.metrics)
              .collect {
                case m @ (key, metric) if (key == "numFiles" && metric.value == cond.numFiles.get) =>
                  m
              }
            println("check condition: " + cond.condition)
            assert(numFilesMetric.size == 1)
          }
          checkAnswer(res, input.filter(cond.condition).collect())
      }
  }

  test("zorder by support map type") {
    val sparkSession = spark
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val input = (1 to 100).map(i => {
      val info = if (i < 50) "a" else "b"
      (i, Map("info" -> info, "name" -> s"name_${i}"))
    }).toDF("col_1", "col_2")

    append(input)
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(Seq("col_2['info']"), 5)

    runZorderByFilterTest(
      table,
      input,
      Seq(
        FilterTestInfo("col_2['info'] == 'a'", 3)
      )
    )
  }

  test("delta filter with no statistics column should work fine.") {
    // todo:(fchen)
  }

  override def sparkConf: SparkConf = {
    val c = super.sparkConf
    c.set("spark.ui.enabled", "true")
    c
  }

  private def exchangeLeftRight(condition: String): String = {
    val opMap = Map(
      ">" -> "<",
      "<" -> ">",
      "=" -> "=",
      "==" -> "==",
      ">=" -> "<=",
      "<=" -> ">=",
      "<=>" -> "<=>",
      "<>" -> "<>"
    )
    val arr = condition.split(" ")
      .filter(_ != "")

    val opIndex = arr.zipWithIndex.filter {
      case (op, index) => opMap.keySet.contains(op)
    }

    val res = arr.toBuffer.asJava
    opIndex.foreach {
      case (op, index) =>
        Collections.swap(res, index - 1, index + 1)
        res.set(index, opMap(op))
    }
    res.asScala.toArray.mkString(" ")
  }
}

case class FilterTestInfo(condition: String, numFiles: Option[Int])

object FilterTestInfo {
  def apply(condition: String, numFiles: Int): FilterTestInfo = {
    new FilterTestInfo(condition, Option(numFiles))
  }

}

//object ScalaOptimizeSuite extends OptimizeSuiteBase {
//  override protected def executeUpdate(target: String, set: String, where: String): Unit = ???
//}

