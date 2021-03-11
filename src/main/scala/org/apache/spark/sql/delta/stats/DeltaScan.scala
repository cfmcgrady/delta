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

package org.apache.spark.sql.delta.stats

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.functions._
import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.sql.catalyst.plans.logical.DeltaOptimize
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.types.AtomicType
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
    val projection: AttributeSet) extends Logging {

  implicit val enc = SingleAction.addFileEncoder
  val spark = addFiles.sparkSession
  import spark.implicits._

  lazy val statsSchema = {
    val isStatEmpty = addFiles.select("stats").filter($"stats" isNotNull).isEmpty
    if (!isStatEmpty) {
      Option(addFiles.sparkSession.read.json(addFiles.select("stats").as[String]).schema)
    } else {
      None
    }
  }
  // todo:(fchen) handle checkepointv2
  lazy val statDF = {
    statsSchema match {
      case Some(schema) =>
        Option(
          addFiles.withColumn("stats_parsed", from_json($"stats", schema)))
      case _ => None
    }
  }

  lazy val statsSchemaSet = statsSchema.map(SchemaUtils.explodeNestedFieldNames)

  lazy val files: Array[AddFile] = {
    statDF match {
      case Some(df) if dataFilters.nonEmpty =>
        val condition = withNullStatFileCondition(
          dataFilters.map(rewriteDataFilters).reduce(And)
        )
        logInfo(s"delta log filter condition ${condition}")
        df.filter(new Column(condition))
          .as[AddFile]
          .collect
      case _ =>
        addFiles.as[AddFile].collect
    }
  }

  // we should include all addFiles which has a null value stats.
  val withNullStatFileCondition =
    (condition: Expression) => Or(condition, col("stats_parsed").isNull.expr)

  def allFilters: ExpressionSet = partitionFilters ++ dataFilters ++ unusedFilters

  // todo:(fchen) limit support
  /**
   * according the data filter condition, rewrite an new condition for delta log filter.
   */
  def rewriteDataFilters(condition: Expression): Expression = {
    def withNotNullColumnStat(expression: Expression
                             )(filterCondition: (Seq[String]) => Expression): Expression = {
      val colName = DeltaOptimize.getTargetColNameParts(expression)
      val minColNameParts = Seq("minValues") ++ colName
      val minCol = SchemaUtils.prettyFieldName(minColNameParts)
      val parsedMinCol = SchemaUtils.prettyFieldName("stats_parsed" +: minColNameParts)
      statDF match {
        case Some(df) if (statsSchemaSet.get.contains(minCol) &&
          df.selectExpr(parsedMinCol).schema.head.dataType.isInstanceOf[AtomicType]) =>
          filterCondition(colName)
        case _ => Literal(true)
      }
    }

    val minCol = (colName: Seq[String]) =>
      col(SchemaUtils.prettyFieldName(Seq("stats_parsed", "minValues") ++ colName)).expr
    val maxCol = (colName: Seq[String]) =>
      col(SchemaUtils.prettyFieldName(Seq("stats_parsed", "maxValues") ++ colName)).expr
    val nullCol = (colName: Seq[String]) =>
      col(SchemaUtils.prettyFieldName(Seq("stats_parsed", "nullCount") ++ colName)).expr
    val numRecordsCol = col("stats_parsed.numRecords").expr

    condition match {
      case et @ EqualTo(_: AttributeReference |
                        _: GetStructField |
                        _: GetMapValue, right: Literal) =>
        withNotNullColumnStat(et.left) {
          colName =>
            And(LessThanOrEqual(minCol(colName), right), GreaterThanOrEqual(maxCol(colName), right))
        }

      case et @ EqualTo(left: Literal, _: AttributeReference |
                                       _: GetStructField |
                                       _: GetMapValue) =>
        withNotNullColumnStat(et.right) {
          colName =>
            And(
              LessThanOrEqual(minCol(colName), left),
              GreaterThanOrEqual(maxCol(colName), left))
        }

      case ens @ EqualNullSafe(_: AttributeReference |
                               _: GetStructField |
                               _: GetMapValue, _ @ Literal(null, _)) =>
        withNotNullColumnStat(ens.left) {
          colName =>
            GreaterThan(nullCol(colName), lit(0).expr)
        }

      case ens @ EqualNullSafe(_ @ Literal(null, _), _: AttributeReference |
                                                     _: GetStructField |
                                                     _: GetMapValue) =>
        withNotNullColumnStat(ens.right) {
          colName =>
            GreaterThan(nullCol(colName), lit(0).expr)
        }

      // the same with EqualTo
      case ens @ EqualNullSafe(_: AttributeReference |
                               _: GetStructField |
                               _: GetMapValue, right @ NonNullLiteral(_, _)) =>
        withNotNullColumnStat(ens.left) {
          colName =>
            And(
              LessThanOrEqual(minCol(colName), right),
              GreaterThanOrEqual(maxCol(colName), right))
        }
      case ens @ EqualNullSafe(left @ NonNullLiteral(_, _), _: AttributeReference |
                                                            _: GetStructField |
                                                            _: GetMapValue) =>
        withNotNullColumnStat(ens.right) {
          colName =>
            And(
              LessThanOrEqual(minCol(colName), left),
              GreaterThanOrEqual(maxCol(colName), left))
        }

      case lt @ LessThan(_: AttributeReference |
                         _: GetStructField |
                         _: GetMapValue, right: Literal) =>
        withNotNullColumnStat(lt.left) {
          colName => LessThan(minCol(colName), right)
        }

      case lt @ LessThan(left: Literal, _: AttributeReference |
                                        _: GetStructField |
                                        _: GetMapValue) =>
        withNotNullColumnStat(lt.right) {
          colName => GreaterThan(maxCol(colName), left)
        }

      case gt @ GreaterThan(_: AttributeReference |
                            _: GetStructField |
                            _: GetMapValue, right: Literal) =>
        withNotNullColumnStat(gt.left) {
          colName => GreaterThan(maxCol(colName), right)
        }
      case gt @ GreaterThan(left: Literal, _: AttributeReference |
                                           _: GetStructField |
                                           _: GetMapValue) =>
        withNotNullColumnStat(gt.right) {
          colName => LessThan(minCol(colName), left)
        }

      case lteq @ LessThanOrEqual(_: AttributeReference |
                                  _: GetStructField |
                                  _: GetMapValue, right: Literal) =>
        withNotNullColumnStat(lteq.left) {
          colName => LessThanOrEqual(minCol(colName), right)
        }

      case lteq @ LessThanOrEqual(left: Literal, _: AttributeReference |
                                                 _: GetStructField |
                                                 _: GetMapValue) =>
        withNotNullColumnStat(lteq.right) {
          colName => GreaterThanOrEqual(maxCol(colName), left)
        }

      case gteq @ GreaterThanOrEqual(_: AttributeReference |
                                   _: GetStructField |
                                   _: GetMapValue, right: Literal) =>
        withNotNullColumnStat(gteq.left) {
          colName => GreaterThanOrEqual(maxCol(colName), right)
        }

      case gteq @ GreaterThanOrEqual(left: Literal, _: AttributeReference |
                                                    _: GetStructField |
                                                    _: GetMapValue) =>
        withNotNullColumnStat(gteq.right) {
          colName => LessThanOrEqual(minCol(colName), left)
        }

      case isNull @ IsNull(_: AttributeReference | _: GetStructField | _: GetMapValue) =>
        withNotNullColumnStat(isNull.child) {
          colName => GreaterThan(nullCol(colName), lit(0).expr)
        }

      case isNotNull @ IsNotNull(_: AttributeReference | _: GetStructField | _: GetMapValue) =>
        withNotNullColumnStat(isNotNull.child) {
          colName => LessThan(nullCol(colName), numRecordsCol)
        }

      case in@ In(_: AttributeReference | _: GetStructField | _: GetMapValue, list: Seq[Literal]) =>
        withNotNullColumnStat(in.value) {
          colName =>
            list.map(lit => {
              And(
                LessThanOrEqual(minCol(colName), lit),
                GreaterThanOrEqual(maxCol(colName), lit))
            }).reduce(Or)
        }

      case _ @ Not(in @ In(_: AttributeReference |
                           _: GetStructField |
                           _: GetMapValue, list: Seq[Literal])) =>
        withNotNullColumnStat(in.value) {
          colName =>
            // only exclude file which min/max == inValue
            list.map(lit => {
              Not(And(EqualTo(minCol(colName), lit), EqualTo(maxCol(colName), lit)))
            }).reduce(And)
        }

      case not @ Not(et @ EqualTo(_: AttributeReference |
                                  _: GetStructField |
                                  _: GetMapValue, right: Literal)) =>
        withNotNullColumnStat(et.left) {
          colName => Not(And(EqualTo(minCol(colName), right), EqualTo(maxCol(colName), right)))
        }

      case not @ Not(et @ EqualTo(left: Literal, _: AttributeReference |
                                                 _: GetStructField |
                                                 _: GetMapValue)) =>
        withNotNullColumnStat(et.right) {
          colName => Not(And(EqualTo(minCol(colName), left), EqualTo(maxCol(colName), left)))
        }

      case or: Or =>
        val resLeft = rewriteDataFilters(or.left)
        val resRight = rewriteDataFilters(or.right)
        Or(resLeft, resRight)

      case and: And =>
        val resLeft = rewriteDataFilters(and.left)
        val resRight = rewriteDataFilters(and.right)
        And(resLeft, resRight)

      case _ @ StartsWith(attribute, value @ Literal(_: UTF8String, _)) =>
        withNotNullColumnStat(attribute) {
          colName =>
            Or(
              // min <= value && max >= value
              And(
                LessThanOrEqual(minCol(colName), value),
                GreaterThanOrEqual(maxCol(colName), value)),
              // if min/max start with value, we should also return this file.
              Or(
                StartsWith(minCol(colName), value), StartsWith(maxCol(colName), value)))
        }

      case expr: Expression =>
        throw new UnsupportedOperationException(s"unsupported expression: ${expr}")
    }
  }
}
