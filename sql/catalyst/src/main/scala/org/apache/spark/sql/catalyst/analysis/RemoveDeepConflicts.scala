/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSet, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

object RemoveDeepConflicts extends Rule[LogicalPlan] {

  def rewrite(right: LogicalPlan, conflicts: AttributeSet): LogicalPlan = {
    assert(conflicts.forall(_.isInstanceOf[AttributeReference]))
    val newIdMap: Map[ExprId, ExprId] = conflicts.flatMap {
      case a: AttributeReference => Seq(a.exprId -> a.newInstance().exprId)
      case _ => Nil
    }.toMap
    val rewritten = right.transformAllExpressions {
      case e => e.transform {
        case a: AttributeReference if newIdMap.contains(a.exprId) =>
          a.withExprId(newIdMap(a.exprId))
        case a: Alias if newIdMap.contains(a.exprId) =>
          a.copy()(newIdMap(a.exprId), a.qualifier, a.explicitMetadata, a.isGenerated)
      }
    }
    assert(getDescendantOutputs(rewritten).intersect(conflicts).isEmpty)
    rewritten
  }

  def process(left: LogicalPlan, right: LogicalPlan, root: LogicalPlan):
  (LogicalPlan, LogicalPlan) = {
    assert(left.outputSet.intersect(right.outputSet).isEmpty)
    val leftDeepOut = getDescendantOutputs(left)
    val rightDeepOut = getDescendantOutputs(right)
    val newLeft = if (right.outputSet.intersect(leftDeepOut).nonEmpty) {
      val conflicts = right.outputSet.intersect(leftDeepOut)
      logDebug(s"Rewriting left branch (${left.simpleString}) of ${root.simpleString} " +
        s"for conflicts $conflicts")
      rewrite(left, conflicts)
    } else {
      left
    }
    val newRight = if (left.outputSet.intersect(rightDeepOut).nonEmpty) {
      val conflicts = left.outputSet.intersect(rightDeepOut)
      logDebug(s"Rewriting right branch (${right.simpleString}) of ${root.simpleString} " +
        s"for conflicts $conflicts")
      rewrite(right, conflicts)
    } else {
      right
    }
    (newLeft, newRight)
  }

  def getDescendantOutputs(plan: LogicalPlan): AttributeSet = {
    plan.children.map(_.collect{
      case p: LogicalPlan => p.outputSet
    }.fold(AttributeSet.empty)((a, b) => a ++ b)).fold(AttributeSet.empty)((a, b) => a ++ b)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(left, right, _, _) =>
      val (newLeft, newRight) = process(left, right, j)
      j.copy(left = newLeft, right = newRight)
  }
}
