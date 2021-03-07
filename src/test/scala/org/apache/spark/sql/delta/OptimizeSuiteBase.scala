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
    table.optimize(Seq("id"))
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
    table.optimize(expr("pid = 'a'"), Seq("col_1"))
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

  override def sparkConf: SparkConf = {
    val c = super.sparkConf
    c.set("spark.ui.enabled", "true")
    c
  }
}

case class FilterTestInfo(condition: String, numFiles: Int)

//object ScalaOptimizeSuite extends OptimizeSuiteBase {
//  override protected def executeUpdate(target: String, set: String, where: String): Unit = ???
//}

