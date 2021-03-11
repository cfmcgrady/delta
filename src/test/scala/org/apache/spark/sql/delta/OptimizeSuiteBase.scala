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

package org.apache.spark.sql.delta

import java.io.File
import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfterEach

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

  protected def saveTable(
      df: DataFrame,
      mode: String,
      conf: Map[String, String],
      partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("delta").mode(mode)
    conf.foreach {
      case (k, v) => writer.option(k, v)
    }
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    }
    writer.save(deltaLog.dataPath.toString)
  }

  protected def append(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    saveTable(df, "append", Map.empty, partitionBy)
  }

  protected def overwrite(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    saveTable(df, "overwrite", Map("mergeSchema" -> "true"), partitionBy)
  }

  implicit def jsonStringToSeq(json: String): Seq[String] = json.split("\n")

  test("optimize partitioned table basic") {
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
    table.optimize(Seq("col_1"), 16)
    table.optimize(expr("pid = 'a'"), Seq("col_1"), 16)
//    table.toDF.filter("col_1 = 1").show()
    val df2 = table.toDF.filter("col_1 = 2")
    df2.collect()
    df2.queryExecution.executedPlan.collectLeaves().foreach(l => {
      l.metrics.foreach {
        case (key, metric) if (key == "numFiles") =>
          // scalastyle:off println
          println("numFile" + metric.value)
          // scalastyle:on println
        case _ =>
      }
    })
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
                case m @ (key, metric)
                  if (key == "numFiles" && metric.value == cond.numFiles.get) => m
              }
            // scalastyle:off println
            println("check condition: " + cond.condition)
            // scalastyle:on println
            assert(numFilesMetric.size == 1)
          }
          checkAnswer(res, input.filter(cond.condition).collect())
      }
  }

  test("zorder by string column basic case.") {
    val sparkSession = spark
    import sparkSession.implicits._
    val input = (1 to 100).map {
      case i if i % 2 == 0 => null
      case i => i.toString
    }.toDF("col_1")
    append(input)
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(Seq("col_1"), 20)

    val filters = Seq(
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
      FilterTestInfo("col_1 <=> '1'", 1),
      FilterTestInfo("NOT EXISTS (SELECT null)", None)
    )
    runZorderByFilterTest(table, input, filters)

    // column without statistics should also work fine.
    val input2 = input.withColumn("fake_column_with_statistics", lit("fake_value"))
    overwrite(input2)
    table.optimize(Seq("fake_column_with_statistics"), 20)
    val newFilters = filters.map {
      case f if f.numFiles == None => f
      // without column statistics, delta should read all of files, so we set numFiles == 20
      case f: FilterTestInfo => f.copy(numFiles = Option(20))
    }
    runZorderByFilterTest(table, input, newFilters)
  }

  test("filter contains stat/non-stat column.") {
    val sparkSession = spark
    import sparkSession.implicits._
    val input = Seq(("a", "a"), ("b", "b"), ("c", "c")).toDF("col_1", "col_2")
    append(input)
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(Seq("col_1"), 3)
    runZorderByFilterTest(table, input, Seq(FilterTestInfo("col_1 == 'a' or col_2 == 'c'", 3)))
  }


  test("file filter with not in condition") {
    val sparkSession = spark
    import sparkSession.implicits._
    val input = Seq("a", "b", "c").toDF("col_1")
    append(input)
    val table = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    table.optimize(Seq("col_1"), 3)
    runZorderByFilterTest(
      table,
      input,
      Seq(
        FilterTestInfo("col_1 not in ('a', 'b')", 1),
        FilterTestInfo("col_1 <> 'a'", 2)
      )
    )
  }

  test("file filter with non-statistics partititon.") {
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

  test("zorder by support map type") {
    val sparkSession = spark
    import sparkSession.implicits._

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
                case m @ (key, metric)
                  if (key == "numFiles" && metric.value == cond.numFiles.get) => m
              }
            // scalastyle:off println
            println("check condition: " + cond.condition)
            // scalastyle:on println
            assert(numFilesMetric.size == 1)
          }
          checkAnswer(res, input.filter(cond.condition).collect())
      }
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

case class TestCaseClass(info: String)
