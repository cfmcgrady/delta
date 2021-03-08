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
// scalastyle:off
// todo:(fchen) fix style
package org.apache.spark.sql.delta.commands

import java.util.UUID

import scala.collection.mutable.HashMap

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, GetMapValue, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{DeltaUpdateTable, Project}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, DeltaOptions, DeltaTableIdentifier, DeltaTableUtils, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.util.JsonUtils

/**
 * Used to Optimize a delta table.
 */
case class OptimizeCommand(
    tahoeFileIndex: TahoeFileIndex,
    condition: Option[Expression],
    zorderBy: Seq[Expression],
    outputFileNum: Int)
  extends RunnableCommand
  with DeltaCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {

//    val tablePath = DeltaTableIdentifier(sparkSession, tableId) match {
//      case Some(id) if id.path.isDefined =>
//        new Path(id.path.get)
//      case _ =>
//        new Path(sparkSession.sessionState.catalog.getTableMetadata(tableId).location)
//    }


//    val deltaLog = DeltaLog.forTable(sparkSession, tablePath)
    val deltaLog = tahoeFileIndex.deltaLog

    if (deltaLog.snapshot.version < 0) {
      throw DeltaErrors.notADeltaTableException("OPTIMIZE")
    }

    deltaLog.withNewTransaction { txn =>
//      val actions = write(txn, sparkSession)
//      val operation = DeltaOperations.Write(mode, Option(partitionColumns),
//        options.replaceWhere, options.userMetadata)
//      txn.commit(actions, operation)
      val actions = performOptimize(sparkSession, deltaLog, txn)
      txn.commit(actions, DeltaOperations.Optimize())
    }
    Seq.empty
  }

  def performOptimize(sparkSession: SparkSession,
                      deltaLog: DeltaLog,
                      txn: OptimisticTransaction): Seq[FileAction] = {
    val partitionFilter = condition.map { filter =>
      val (partitionPredicates, dataPredicates) = DeltaTableUtils.splitMetadataAndDataPredicates(
        filter, deltaLog.snapshot.metadata.partitionColumns, sparkSession)

      if (dataPredicates.nonEmpty) {
        throw DeltaErrors.optimizeContainsDataFilterError(
          "search condition of OPTIMIZE operation", filter)
      }
      partitionPredicates
    }.getOrElse(Nil)

    val allFiles = txn.filterFiles(partitionFilter)
    val df = txn.deltaLog.createDataFrame(txn.snapshot, allFiles)
    val zorderByCols = zorderBy.map(c => DeltaUpdateTable.getTargetColNameParts(c).mkString("."))
    val indexDF = ZIndexUtil.createZIndex(df, zorderByCols, outputFileNum)
    val actions = txn.writeFiles(indexDF)

    // normilize input file name
    val nameToAddFile = generateCandidateFileMap(deltaLog.dataPath, actions.collect {case add: AddFile => add})

    val getNormalizedFileName = (absolutePath: String) => getTouchedFile(deltaLog.dataPath, absolutePath, nameToAddFile)

    val addFiles = actions.collect {case add: AddFile => add}

    val newDF = txn.deltaLog.createDataFrame(txn.snapshot, addFiles)

    // todo:(fchen) 转join?

    val statistics = collectTableStatistics(sparkSession, zorderByCols, newDF, getNormalizedFileName)
      .map(fs => (fs.file, fs))
      .toMap

    val addActions = actions.map {
      case addFile: AddFile =>
        val stats = JsonUtils.toJson(statistics.get(addFile.path))
//        println(JsonUtils.toPrettyJson(statistics.get(addFile.path)))
        addFile.copy(stats = stats, dataChange = false)
      case action: FileAction => action
    }
    val deleteActions = allFiles.map(_.remove)
    addActions ++ deleteActions
  }

  /**
   * return the table statistics for given input path.
   */
  def collectTableStatistics(spark: SparkSession,
                             zorderByColumns: Seq[String],
                             data: DataFrame,
                             getNormalizedFileName: (String) => AddFile): Array[FileStatistics] = {
    val tempView = s"__cache_${UUID.randomUUID().toString.replace("-", "")}"

//    val tempDF = spark.read
//      .format(inputFormat)
//      .load(inputPath)
    val tempDF = data
    tempDF.createOrReplaceTempView(tempView)

    val minMaxExpr = zorderByColumns.map {
      c =>
        s"min(${c}) as ${minColName(c)}, max($c) as ${maxColName(c)}"
    }.mkString(",")

    val countExpr = zorderByColumns.map {
      c => s"count($c) as ${countColName(c)}"
    }.mkString(",")

    val stat = spark.sql(
      s"""
         |SELECT file, count(*) as numRecords, ${countExpr}, ${minMaxExpr}
         |FROM (
         | SELECT input_file_name() AS file, * FROM ${tempView}
         |) GROUP BY file
         |""".stripMargin)

    stat.collect()
      .map(r => {
        val numRecords = r.getAs[Long]("numRecords")
        val (minValues, maxValues, nullCountValues) = zorderByColumns.map(c => {
          val normalizedColName = getNormalizedColumnName(tempDF, c)
          val min: Any = Option(r.get(r.fieldIndex(minColName(c))))
          val max: Any = Option(r.get(r.fieldIndex(maxColName(c))))
          val numNonNullRecords = r.getAs[Long](countColName(c))
          val nullCount: Any = numRecords - numNonNullRecords
//          (
//            normalizedColName.foldRight(min)((cn, min) => Map(cn -> min)),
//            normalizedColName.foldRight(max)((cn, max) => Map(cn -> max)),
//            normalizedColName.foldRight(nullCount)((cn, nc) => Map(cn -> nc)))
          normalizedColName.foldRight((min, max, nullCount)) {
            case (c, (min, max, nullCount)) =>
              (Map(c -> min), Map(c -> max), Map(c -> nullCount))
          }
        }).unzip3

        val fileName = getNormalizedFileName(r.getAs[String]("file"))
        FileStatistics(
          fileName.path,
          numRecords,
          minValues.asInstanceOf[Seq[Map[String, Any]]].reduce(_ ++ _),
          maxValues.asInstanceOf[Seq[Map[String, Any]]].reduce(_ ++ _),
          nullCountValues.asInstanceOf[Seq[Map[String, Any]]].reduce(_ ++ _)
        )
      })
  }

//  /**
//   * Scan all the affected files and write out the optimized files
//   */
//  private def rewriteFiles(
//      spark: SparkSession,
//      txn: OptimisticTransaction,
//      rootPath: Path,
//      inputLeafFiles: Seq[String],
//      nameToAddFileMap: Map[String, AddFile]): Seq[FileAction] = {
//    // Containing the map from the relative file path to AddFile
//    val baseRelation = buildBaseRelation(
//      spark, txn, "optimize", rootPath, inputLeafFiles, nameToAddFileMap)
//    val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
//    val targetDf = Dataset.ofRows(spark, newTarget)
//    val updatedDataFrame = {
//      val updatedColumns = buildUpdatedColumns(condition)
//      targetDf.select(updatedColumns: _*)
//    }
//
//    txn.writeFiles(updatedDataFrame)
//  }

  def extractRecursively(expr: Expression): Seq[String] = expr match {
    case attr: Attribute => Seq(attr.name)

    case Alias(c, _) => extractRecursively(c)

    case GetStructField(c, _, Some(name)) => extractRecursively(c) :+ name

    case GetMapValue(left, lit: Literal) => extractRecursively(left) :+ lit.eval().toString

    //    case _: ExtractValue =>
    //      throw new RuntimeException("extract nested fields is only supported for StructType.")
    case other =>
      throw new RuntimeException(
        s"Found unsupported expression '$other' while parsing target column name parts")
  }

  private def getNormalizedColumnName(df: DataFrame, colName: String): Seq[String] = {
    df.selectExpr(colName)
      .queryExecution
      .analyzed
      .collect {case proj: Project => proj}
      .headOption
      .map(proj => {
        extractRecursively(proj.projectList.head)
      })
      .getOrElse {
        throw new RuntimeException(s"can't normalize the ${colName}")
      }
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

  private val colToIndexColName = HashMap[String, String]()

  private val indexColName = {
    (colName: String) =>
      colToIndexColName.getOrElseUpdate(
        colName,
        s"__${UUID.randomUUID().toString.replace("-", "")}__"
      )
  }
}

case class FileStatistics(
   file: String,
   numRecords: Long,
   minValues: Map[String, Any],
   maxValues: Map[String, Any],
   nullCount: Map[String, Any])

