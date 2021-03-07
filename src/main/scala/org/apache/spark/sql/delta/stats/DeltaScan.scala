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

    if (!addFiles.select("stats").filter($"stats" isNotNull).isEmpty) {
      addFiles.select("stats").filter($"stats" isNotNull)
          .show
      addFiles.alias("l")
        .join(
          addFiles.sparkSession.read.json(addFiles.select("stats").as[String]).alias("r"),
          $"l.path" === $"r.file",
          "left"
        ).select($"l.*")
        .show(truncate = false)
    }
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
          .show()
        df.filter(new Column(condition))
          .select($"l.*")
          .as[AddFile]
          .collect
      case _ =>
        println("stat df is empty")
        println(statDF.isEmpty)
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
    condition match {
      case et @ EqualTo(_: AttributeReference |
                        _: GetStructField |
                        _: GetMapValue, right: Literal) =>
        val colName = DeltaUpdateTable.getTargetColNameParts(et.left)
        And(
          LessThanOrEqual(minCol(colName), right),
          GreaterThanOrEqual(maxCol(colName), right))
      case x: Expression =>
//        println(x.getClass.getCanonicalName)
//        throw new RuntimeException
        Literal(true)
    }
  }
//  def findTouchFileByExpression(condition: Expression): Array[FileStatistics] = {
//
//    def equalTo(left: Expression, right: Literal): Array[FileStatistics] = {
//      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
//      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
//      fileMetadata.filter {
//        case fileStatistics =>
//          fileStatistics.columnStatistics
//            .get(colName).forall {
//            // `any value = null` return null in spark sql, and this expression will be optimize by catalyst
//            // (means this operation will be execute by spark engine).
//            // so we don't consider null value for right expression.
//            case ColumnStatistics(_, None, None, _) =>
//              false
//            case ColumnStatistics(_, Some(min), Some(max), _) =>
//              ordering.lteq(Literal.create(min).value, right.value) && ordering.gteq(Literal.create(max).value, right.value)
//          }
//      }
//    }
//
//    def equalNullSafe(left: Expression, right: Literal): Array[FileStatistics] = {
//      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
//      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
//      fileMetadata.filter {
//        case fileStatistics =>
//          fileStatistics.columnStatistics
//            .get(colName).forall {
//            // start--- right == null case --- //
//            case ColumnStatistics(_, _, _, true) if Option(right.value).isEmpty =>
//              true
//            case ColumnStatistics(_, _, _, false) if Option(right.value).isEmpty =>
//              false
//            // end--- right == null case --- //
//            // right not null case, the same with EqualTo
//            case ColumnStatistics(_, None, None, _) =>
//              false
//            case ColumnStatistics(_, Some(min), Some(max), _) =>
//              ordering.lteq(Literal.create(min).value, right.value) && ordering.gteq(Literal.create(max).value, right.value)
//          }
//      }
//    }
//
//    def lessThan(left: Expression, right: Literal): Array[FileStatistics] = {
//      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
//      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
//      fileMetadata.filter {
//        case fileStatistics =>
//          fileStatistics.columnStatistics
//            .get(colName)
//            .forall {
//              // the same with EqualTo, we don't need consider null value for right Expression.
//              case ColumnStatistics(_, None, None, _) =>
//                false
//              case ColumnStatistics(_, Some(min), _, _) =>
//                ordering.lt(Literal(min).value, right.value)
//            }
//      }
//    }
//
//    def lessThanOrEqual(left: Expression, right: Literal): Array[FileStatistics] = {
//      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
//      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
//      fileMetadata.filter {
//        case fileStatistics =>
//          fileStatistics.columnStatistics
//            .get(colName)
//            .forall {
//              // the same with EqualTo, we don't need consider null value for right Expression.
//              case ColumnStatistics(_, None, None, _) =>
//                false
//              case ColumnStatistics(_, Some(min), _, _) =>
//                ordering.lteq(Literal(min).value, right.value)
//            }
//      }
//    }
//
//    def greaterThan(left: Expression, right: Literal): Array[FileStatistics] = {
//      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
//      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
//      fileMetadata.filter {
//        case fileStatistics =>
//          fileStatistics.columnStatistics
//            .get(colName)
//            .forall {
//              // the same with EqualTo, we don't need consider null value for right Expression.
//              case ColumnStatistics(_, None, None, _) =>
//                false
//              case ColumnStatistics(_, _, Some(max), _) =>
//                ordering.gt(Literal(max).value, right.value)
//            }
//      }
//    }
//
//    def greaterThanOrEqual(left: Expression, right: Literal): Array[FileStatistics] = {
//      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
//      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
//      fileMetadata.filter {
//        case fileStatistics =>
//          fileStatistics.columnStatistics
//            .get(colName)
//            .forall {
//              // the same with EqualTo, we don't need consider null value for right Expression.
//              case ColumnStatistics(_, None, None, _) =>
//                false
//              case ColumnStatistics(_, _, Some(max), _) =>
//                ordering.gteq(Literal(max).value, right.value)
//            }
//      }
//    }
//
//    condition match {
//      case et @ EqualTo(_: AttributeReference |
//                        _: GetStructField |
//                        _: GetMapValue, right: Literal) =>
//        equalTo(et.left, right)
//
//      case et @ EqualTo(left: Literal, _: AttributeReference |
//                                       _: GetStructField |
//                                       _: GetMapValue) =>
//        equalTo(et.right, left)
//
//      case lt @ LessThan(_: AttributeReference |
//                         _: GetStructField |
//                         _: GetMapValue, right: Literal) =>
//        lessThan(lt.left, right)
//      case lt @ LessThan(left: Literal, _: AttributeReference |
//                                        _: GetStructField |
//                                        _: GetMapValue) =>
//        greaterThan(lt.right, left)
//      case lteq @ LessThanOrEqual(_: AttributeReference |
//                                  _: GetStructField |
//                                  _: GetMapValue, right: Literal) =>
//        lessThanOrEqual(lteq.left, right)
//
//      case lteq @ LessThanOrEqual(left: Literal, _: AttributeReference |
//                                                 _: GetStructField |
//                                                 _: GetMapValue) =>
//        greaterThanOrEqual(lteq.right, left)
//
//      case gt @ GreaterThan(_: AttributeReference |
//                            _: GetStructField |
//                            _:GetMapValue, right: Literal) =>
//        greaterThan(gt.left, right)
//
//      case gt @ GreaterThan(left: Literal, _: AttributeReference |
//                                           _: GetStructField |
//                                           _: GetMapValue) =>
//        lessThan(gt.right, left)
//
//      case gt @ GreaterThanOrEqual(_: AttributeReference |
//                                   _: GetStructField |
//                                   _: GetMapValue, right: Literal) =>
//        greaterThanOrEqual(gt.left, right)
//
//      case gt @ GreaterThanOrEqual(left: Literal, _: AttributeReference |
//                                                  _: GetStructField |
//                                                  _: GetMapValue) =>
//        lessThanOrEqual(gt.right, left)
//      case isNull @ IsNull(_: AttributeReference | _: GetStructField | _: GetMapValue) =>
//        val colName = ZIndexUtil.extractRecursively(isNull.child).mkString(".")
//        fileMetadata.filter {
//          case fileStatistics =>
//            fileStatistics.columnStatistics
//              .get(colName)
//              .forall {
//                case ColumnStatistics(_, _, _, containsNull) => containsNull
//              }
//        }
//
//      case isNotNull@ IsNotNull(_: AttributeReference | _: GetStructField | _: GetMapValue) =>
//        val colName = ZIndexUtil.extractRecursively(isNotNull.child).mkString(".")
//        fileMetadata.filter {
//          case fileStatistics =>
//            fileStatistics.columnStatistics
//              .get(colName)
//              .forall {
//                case ColumnStatistics(_, _, _, containsNull) => !containsNull
//              }
//        }
//
//      case _ @ StartsWith(attribute, value @ Literal(_: UTF8String, _)) =>
//        val colName = ZIndexUtil.extractRecursively(attribute).mkString(".")
//        val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(StringType)
//        fileMetadata.filter {
//          case fileStatistics =>
//            fileStatistics.columnStatistics
//              .get(colName).forall {
//              case ColumnStatistics(_, None, None, _) =>
//                false
//              case ColumnStatistics(_, Some(min: String), Some(max: String), _) =>
//                val minLit = Literal(min)
//                val maxLit = Literal(max)
//                ordering.lteq(minLit.value, value.value) && ordering.gteq(maxLit.value, value.value) ||
//                  // if min/max start with value, we should also return this file.
//                  minLit.value.asInstanceOf[UTF8String].startsWith(value.value.asInstanceOf[UTF8String]) ||
//                  maxLit.value.asInstanceOf[UTF8String].startsWith(value.value.asInstanceOf[UTF8String])
//            }
//        }
//
//      case in@ In(_: AttributeReference | _: GetStructField | _: GetMapValue, list: Seq[Literal]) =>
//        list.flatMap {
//          // `column in (null)` will not transform to psychical plan, just exclude null value and return empty array.
//          case Literal(null, _) => Array.empty[FileStatistics]
//          case lit: Literal =>
//            equalTo(in.value, lit)
//        }.distinct
//          .toArray
//      case ens @ EqualNullSafe(_: AttributeReference |
//                               _: GetStructField |
//                               _: GetMapValue, right: Literal) =>
//        equalNullSafe(ens.left, right)
//      case ens @ EqualNullSafe(left: Literal, _: AttributeReference |
//                                              _: GetStructField |
//                                              _: GetMapValue) =>
//        equalNullSafe(ens.right, left)
//      case a: And =>
//        val resLeft = findTouchFileByExpression(a.left)
//        val resRight = findTouchFileByExpression(a.right)
//        // 并集
//        resLeft.intersect(resRight)
//      case or: Or =>
//        val resLeft = findTouchFileByExpression(or.left)
//        val resRight = findTouchFileByExpression(or.right)
//        // 并集
//        resLeft.union(resRight).distinct
//
//      case not @ Not(in @ In(_: AttributeReference | _: GetStructField | _: GetMapValue, list: Seq[Literal])) =>
//        val colName = ZIndexUtil.extractRecursively(in.value).mkString(".")
//        val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(in.value.dataType)
//        list.flatMap {
//          // `column in (null)` will not transform to psychical plan, just exclude null value and return empty array.
//          case Literal(null, _) => return Array.empty[FileStatistics]
//          case lit: Literal =>
//            fileMetadata.filter {
//              case fileStatistics =>
//                fileStatistics.columnStatistics
//                  .get(colName).forall {
//                  // `any value = null` return null in spark sql, and this expression will be optimize by catalyst
//                  // (means this operation will be execute by spark engine).
//                  // so we don't consider null value for right expression.
//                  case ColumnStatistics(_, None, None, _) =>
//                    false
//                  case ColumnStatistics(_, Some(min), Some(max), _) =>
//                    !(ordering.equiv(Literal(min).value, lit.value) && ordering.equiv(Literal(max).value, lit.value))
//                }
//            }
//        }.distinct
//          .toArray
//    }
//
//  }
}
