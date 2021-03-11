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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, GetMapValue, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.AnalysisException

/**
 * delta optimize command logical plan.
 */
case class DeltaOptimize(
    child: LogicalPlan,
    condition: Option[Expression],
    zorderBy: Seq[NamedExpression],
    outputFileNum: Int)
  extends UnaryNode {
  override def output: Seq[Attribute] = Seq.empty
}

object DeltaOptimize {
  /**
   * reference to
   * [[org.apache.spark.sql.catalyst.plans.logical.DeltaUpdateTable.getTargetColNameParts()]].
   *
   * Extracts name parts from a resolved expression,
   * but we should support [[GetMapValue]] in optimize operation.
   *
   * @param resolvedTargetCol
   * @param errMsg
   * @return
   */
  def getTargetColNameParts(resolvedTargetCol: Expression, errMsg: String = null): Seq[String] = {

    def fail(extraMsg: String): Nothing = {
      val msg = Option(errMsg).map(_ + " - ").getOrElse("") + extraMsg
      throw new AnalysisException(msg)
    }

    def extractRecursively(expr: Expression): Seq[String] = expr match {
      case attr: Attribute => Seq(attr.name)

      case Alias(c, _) => extractRecursively(c)

      case GetStructField(c, _, Some(name)) => extractRecursively(c) :+ name

      case GetMapValue(left, lit: Literal) => extractRecursively(left) :+ lit.eval().toString

      case other =>
        fail(s"Found unsupported expression '$other' while parsing target column name parts")
    }

    extractRecursively(resolvedTargetCol)
  }
}
