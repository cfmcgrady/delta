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
package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.functions._
import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.plans.logical.DeltaUpdateTable
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.unsafe.types.UTF8String

/**
 * Note: Please don't add any new constructor to this class. `jackson-module-scala` always picks up
 * the first constructor returned by `Class.getConstructors` but the order of the constructors list
 * is non-deterministic. (SC-13343)
 */
case class DataSize(
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    bytesCompressed: Option[Long] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    rows: Option[Long] = None)

object DataSize {
  def apply(a: ArrayAccumulator): DataSize = {
    DataSize(
      Option(a.value(0)).filterNot(_ == -1),
      Option(a.value(1)).filterNot(_ == -1))
  }
}

/**
 * Used to hold details the files and stats for a scan where we have already
 * applied filters and a limit.
 */
case class DeltaScan(
    version: Long,
    // 这里的文件已经经过分区过滤
    addFiles: DataFrame,
    total: DataSize,
    partition: DataSize,
    scanned: DataSize)(
    // Moved to separate argument list, to not be part of case class equals check -
    // expressions can differ by exprId or ordering, but as long as same files are scanned, the
    // PreparedDeltaFileIndex and HadoopFsRelation should be considered equal for reuse purposes.
    val partitionFilters: ExpressionSet,
    val dataFilters: ExpressionSet,
    val unusedFilters: ExpressionSet,
    val projection: AttributeSet) {

  implicit val enc = SingleAction.addFileEncoder
  val spark = addFiles.sparkSession
  import spark.implicits._

  // todo:(fchen) handle checkepointv2
  lazy val statDF = {
    println("add files ------")

    val isStatEmpty = addFiles.select("stats").filter($"stats" isNotNull).isEmpty
    // todo:(fchen) do we have batter method?
    if (!isStatEmpty) {
      Option(
        addFiles.alias("l")
          .join(
            addFiles.sparkSession.read.json(addFiles.select("stats").as[String]).alias("r"),
            $"l.path" === $"r.file",
            "left")
          .select($"l.*")
      )
    } else {
      None
    }
  }

//  val inCacheColumns = SchemaUtils.explodeNestedFieldNames(statDF.schema)

  def files: Array[AddFile] = {
    statDF match {
      case Some(df) if dataFilters.nonEmpty =>
        val condition = withNullStatFileCondition(
          dataFilters.map(rewriteDataFilters).reduce(And)
        )
        df.filter(new Column(condition))
          .select($"l.*")
          .as[AddFile]
          .collect
      case _ =>
        addFiles.as[AddFile].collect
    }
//    if (dataFilters.nonEmpty && statDF.isDefined) {
//      val condition = dataFilters.map(rewriteDataFilters).reduce(And)
//      statDF
//      addFiles.as[AddFile].collect
//    } else {
//      addFiles.as[AddFile].collect
//    }
  }

  // we should include all addFiles which has a null value stats.
  val withNullStatFileCondition =
    (condition: Expression) => Or(condition, col("l.stats").isNull.expr)

  def allFilters: ExpressionSet = partitionFilters ++ dataFilters ++ unusedFilters

  // todo:(fchen) limit support
  /**
   * 根据输入改过滤条件，用于过滤文件
   * Not场景下，多分区怎么处理？
   */
  def rewriteDataFilters(condition: Expression): Expression = {
    val minCol = (colName: Seq[String]) => col(("minValues" +: colName).mkString(".")).expr
    val maxCol = (colName: Seq[String]) => col(("maxValues" +: colName).mkString(".")).expr
    val nullCol = (colName: Seq[String]) => col(("nullCount" +: colName).mkString(".")).expr
    val numRecordsCol = col("numRecords").expr
    condition match {
        // todo: (fchen) 通过匹配，简化(unapply)
      case et @ EqualTo(_: AttributeReference |
                        _: GetStructField |
                        _: GetMapValue, right: Literal) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(et.left)
        And(
          LessThanOrEqual(minCol(colName), right),
          GreaterThanOrEqual(maxCol(colName), right))

      case et @ EqualTo(left: Literal, _: AttributeReference |
                                       _: GetStructField |
                                       _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(et.right)
        And(
          LessThanOrEqual(minCol(colName), left),
          GreaterThanOrEqual(maxCol(colName), left))

      case ens @ EqualNullSafe(_: AttributeReference |
                               _: GetStructField |
                               _: GetMapValue, _ @ Literal(null, _)) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(ens.left)
        GreaterThan(nullCol(colName), lit(0).expr)
      case ens @ EqualNullSafe(_ @ Literal(null, _), _: AttributeReference |
                                                     _: GetStructField |
                                                     _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(ens.right)
        GreaterThan(nullCol(colName), lit(0).expr)

        // the same with EqualTo
      case ens @ EqualNullSafe(_: AttributeReference |
                               _: GetStructField |
                               _: GetMapValue, right @ NonNullLiteral(_, _)) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(ens.left)
        And(
          LessThanOrEqual(minCol(colName), right),
          GreaterThanOrEqual(maxCol(colName), right))
      case ens @ EqualNullSafe(left @ NonNullLiteral(_, _), _: AttributeReference |
                                                            _: GetStructField |
                                                            _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(ens.right)
        And(
          LessThanOrEqual(minCol(colName), left),
          GreaterThanOrEqual(maxCol(colName), left))

      case lt @ LessThan(_: AttributeReference |
                         _: GetStructField |
                         _: GetMapValue, right: Literal) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(lt.left)
        LessThan(minCol(colName), right)

      case lt @ LessThan(left: Literal, _: AttributeReference |
                                        _: GetStructField |
                                        _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(lt.right)
        GreaterThan(maxCol(colName), left)

      case gt @ GreaterThan(_: AttributeReference |
                            _: GetStructField |
                            _:GetMapValue, right: Literal) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(gt.left)
        GreaterThan(maxCol(colName), right)
      case gt @ GreaterThan(left: Literal, _: AttributeReference |
                                           _: GetStructField |
                                           _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(gt.right)
        LessThan(minCol(colName), left)
      //        lessThan(gt.right, left)

      case lteq @ LessThanOrEqual(_: AttributeReference |
                                  _: GetStructField |
                                  _: GetMapValue, right: Literal) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(lteq.left)
        LessThanOrEqual(minCol(colName), right)

      case lteq @ LessThanOrEqual(left: Literal, _: AttributeReference |
                                                 _: GetStructField |
                                                 _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(lteq.right)
        GreaterThanOrEqual(maxCol(colName), left)

      case gteq @ GreaterThanOrEqual(_: AttributeReference |
                                   _: GetStructField |
                                   _: GetMapValue, right: Literal) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(gteq.left)
        GreaterThanOrEqual(maxCol(colName), right)

      case gteq @ GreaterThanOrEqual(left: Literal, _: AttributeReference |
                                                    _: GetStructField |
                                                    _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(gteq.right)
        LessThanOrEqual(minCol(colName), left)

      case isNull @ IsNull(_: AttributeReference | _: GetStructField | _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(isNull.child)
        GreaterThan(nullCol(colName), lit(0).expr)

      case isNotNull @ IsNotNull(_: AttributeReference | _: GetStructField | _: GetMapValue) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(isNotNull.child)
        LessThan(nullCol(colName), numRecordsCol)

      case in@ In(_: AttributeReference | _: GetStructField | _: GetMapValue, list: Seq[Literal]) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(in.value)
        list.map(lit => {
          And(
            LessThanOrEqual(minCol(colName), lit),
            GreaterThanOrEqual(maxCol(colName), lit))
        }).reduce(Or)

      case not @ Not(in @ In(_: AttributeReference | _: GetStructField | _: GetMapValue, list: Seq[Literal])) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(in.value)
        // only exclude file which min/max == inValue
        list.map(lit => {
          Not(And(EqualTo(minCol(colName), lit), EqualTo(maxCol(colName), lit)))
        }).reduce(And)

      case not @ Not(et @ EqualTo(_: AttributeReference |
                                  _: GetStructField |
                                  _: GetMapValue, right: Literal)) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(et.left)
        Not(And(EqualTo(minCol(colName), right), EqualTo(maxCol(colName), right)))

      case not @ Not(et @ EqualTo(left: Literal, _: AttributeReference |
                                                 _: GetStructField |
                                                 _: GetMapValue)) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(et.right)
        Not(And(EqualTo(minCol(colName), left), EqualTo(maxCol(colName), left)))

      case or: Or =>
        val resLeft = rewriteDataFilters(or.left)
        val resRight = rewriteDataFilters(or.right)
        Or(resLeft, resRight)

      case a: And =>
        val resLeft = rewriteDataFilters(a.left)
        val resRight = rewriteDataFilters(a.right)
        And(resLeft, resRight)

      case _ @ StartsWith(attribute, value @ Literal(_: UTF8String, _)) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(attribute)
        Or(
          // min <= value && max >= value
          And(LessThanOrEqual(minCol(colName), value), GreaterThanOrEqual(maxCol(colName), value)),
          // if min/max start with value, we should also return this file.
          Or(
            StartsWith(minCol(colName), value), StartsWith(maxCol(colName), value)))

      case expr: Expression =>
        throw new UnsupportedOperationException(s"unsupported expression: ${expr}")
    }
  }
}

object ParsedColumnName {
  // todo:(fchen)优化表达式？
  def unapply(expr: Expression): Option[Seq[String]] = {
    Option(DeltaUpdateTable.getTargetColNameParts(expr))
  }
}

object UnParsedColumn {
  
}
