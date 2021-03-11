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

import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{DeltaOptimize, DeltaUpdateTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.commands.OptimizeCommand
import org.apache.spark.sql.internal.SQLConf

/**
 * Preprocess the [[DeltaDelete]] plan to convert to [[DeleteCommand]].
 */
case class PreprocessTableOptimize(conf: SQLConf) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case d: DeltaOptimize if d.resolved =>
        d.condition.foreach { cond =>
          if (SubqueryExpression.hasSubquery(cond)) {
            throw DeltaErrors.subqueryNotSupportedException("OPTIMIZE", cond)
          }
        }
        PreprocessTableOptimize.toCommand(d)
    }
  }
}
object PreprocessTableOptimize {

  def toCommand(d: DeltaOptimize): OptimizeCommand = EliminateSubqueryAliases(d.child) match {
    case DeltaFullTable(tahoeFileIndex) =>
      OptimizeCommand(tahoeFileIndex, d.condition, d.zorderBy, d.outputFileNum)
    case o =>
      throw DeltaErrors.notADeltaSourceException("OPTIMIZE", Some(o))
  }

  /** Resolve all the references of target columns and condition using the given `resolver` */
  def resolveReferences(optimize: DeltaOptimize,
                        resolver: Expression => Expression): DeltaOptimize = {
    if (optimize.resolved) return optimize
    assert(optimize.child.resolved)

    val DeltaOptimize(child, condition, zorderBy, outputFileNum) = optimize

    val cleanedUpAttributes = zorderBy.map { unresolvedExpr =>
      // Keep them unresolved but use the cleaned-up name parts from the resolved
      val errMsg = s"Failed to resolve ${unresolvedExpr} given columns " +
        s"[${child.output.map(_.qualifiedName).mkString(", ")}]."
      val resolveNameParts =
        DeltaUpdateTable.getTargetColNameParts(resolver(unresolvedExpr), errMsg)
      UnresolvedAttribute(resolveNameParts)
    }

    optimize.copy(
      condition = condition.map(resolver),
      zorderBy = cleanedUpAttributes)
  }
}
