/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.analysis.RewriteMergeIntoTable.{buildMergeRowsOutput, buildMergingOutput}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Expression, Literal, NamedExpression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, MergeIntoIcebergTable, MergeRows, Project, ReplaceIcebergData, V2WriteCommandLike, WriteIcebergDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.OPERATION_COLUMN
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, WriteDeltaProjections}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.write.{DeltaWrite, LogicalWriteInfoImpl, RowLevelOperationTable, SupportsDelta, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

import java.util.UUID
import scala.collection.mutable



object RewriteMergeIntoOperationForRowLineage extends RewriteRowLevelIcebergCommand {

  private val ROW_LINEAGE_ATTRIBUTES = Set(
    "_row_id", "_last_updated_sequence_number")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    RewriteMergeIntoTable.apply(plan) resolveOperators {
      case m@MergeIntoIcebergTable(_, _, _, _, _, Some(_)) =>
        if (rowLineageMetadataAttributes(m.metadataOutput).isEmpty) {
          m
        } else {
          case writeOp: V2WriteCommandLike if !tableAlreadyModifiedForRowLineage(writeOp.table) => {
            val updatedPlan = writeOp match {
              case w: WriteIcebergDelta => updateDeltaPlan(w)
              case r: ReplaceIcebergData => updateReplaceDataPlan(r)
            }
            m.copy(rewritePlan = Some(updatedPlan))
          }

          case _: WriteIcebergDelta | _: ReplaceIcebergData =>
            m
        }
      case append@AppendData(table, _, _, _, _, _) =>
        if (append.query.output.exists(attr => attr.name == "_row_id")) {
          append
        } else {
          val dsv2Relation = table.asInstanceOf[DataSourceV2Relation]
          val rowLineageAttributes = rowLineageMetadataAttributes(dsv2Relation.metadataOutput)
          if (rowLineageAttributes.nonEmpty) {
            val additionalRowLineageOutput =
              rowLineageAttributes.zip(Seq("_row_id", "_last_updated_sequence_number"))
                .map { case (expr, name) =>
                  Alias(expr, name)()
                }

            val lineageAttributeRefs = dsv2Relation.metadataOutput.filter(attr => attr.name == "_row_id"
              || attr.name == "_last_updated_sequence_number")
            val project = Project(append.query.output ++ additionalRowLineageOutput, append.query.children.head)
            val updatedAppend = append.withNewTable(
                append.table.asInstanceOf[DataSourceV2Relation]
                  .copy(output = dsv2Relation.output ++ lineageAttributeRefs))
              .withNewQuery(project)

            updatedAppend
          } else {
            append
          }
        }
    }
  }

  private def tableAlreadyModifiedForRowLineage(tbl: NamedRelation): Boolean = {
    tbl.output.exists(attr => attr.name == "_row_id")
  }

  private def updateDeltaPlan(writeIcebergDelta: WriteIcebergDelta): LogicalPlan = {
    val updatedMergeRows = writeIcebergDelta.query match {
      case mergeRows@MergeRows(_, _, _, matchedOutputs, _, notMatchedOutputs, _, _, _, _, _) => {
        val rowIdOrdinal = matchedOutputs(0)(0)
          .indexWhere(attr => attr.isInstanceOf[Attribute] && attr.asInstanceOf[Attribute].name == "_row_id")
        val lastUpdatedOrdinal = matchedOutputs(0)(0)
          .indexWhere(attr => attr.isInstanceOf[Attribute]
            && attr.asInstanceOf[Attribute].name == "_last_updated_sequence_number")
          val updatedMatchOutputs =
            Seq(
              Seq(matchedOutputs(0)(0),
                // Set the metadata attribute to itself so it's preserved
                matchedOutputs(0)(1).updated(rowIdOrdinal, matchedOutputs(0)(0)(rowIdOrdinal))
                  .updated(lastUpdatedOrdinal, Literal(null)))
            )
          val updatedNotMatchedOutputs = if (notMatchedOutputs.nonEmpty) {
            Seq(notMatchedOutputs(0)
              .updated(rowIdOrdinal, Literal(null)).updated(lastUpdatedOrdinal, Literal(null)))
          } else {
            notMatchedOutputs
          }

          mergeRows.copy(matchedOutputs = updatedMatchOutputs, notMatchedOutputs = updatedNotMatchedOutputs)
        }
      case p => throw new UnsupportedOperationException(s"Unsupported $p")
    }

    val projections = writeIcebergDelta.projections
    val rowLineageAttributes = writeIcebergDelta.originalTable.metadataOutput.filter(attr => attr.name == "_row_id"
      || attr.name == "_last_updated_sequence_number")
    val projectedAttrs = writeIcebergDelta.table.output ++ rowLineageAttributes
    val outputAttrs = updatedMergeRows.output
    val outputs = updatedMergeRows.matchedOutputs.flatten ++ updatedMergeRows.notMatchedOutputs
    val projectedOrdinals = projectedAttrs.map(attr => outputAttrs.indexWhere(_.name == attr.name))

    val structFields = projectedAttrs.zip(projectedOrdinals).map { case (attr, ordinal) =>
      // output attr is nullable if at least one output projection may produce null for that attr
      // but row ID and metadata attrs are projected only for update/delete records and
      // row attrs are projected only in insert/update records
      // that's why the projection schema must rely only on relevant outputs
      // instead of blindly inheriting the output attr nullability
      val nullable = outputs.exists(output => output(ordinal).nullable)
      StructField(attr.name, attr.dataType, nullable, attr.metadata)
    }
    val schema = StructType(structFields)

    val newProjection = ProjectingInternalRow(schema, projectedOrdinals)

    val updatedProjections = WriteDeltaProjections(Some(newProjection),
      projections.rowIdProjection, projections.metadataProjection)
    val res = WriteIcebergDelta(
      writeIcebergDelta.table.asInstanceOf[DataSourceV2Relation]
        .copy(output = writeIcebergDelta.table.asInstanceOf[DataSourceV2Relation].output
          ++ rowLineageAttributes.map(attr => attr.asInstanceOf[AttributeReference])),
      updatedMergeRows,
      writeIcebergDelta.originalTable,
      updatedProjections,
      writeIcebergDelta.write)
    res
  }

  private def updateReplaceDataPlan(replaceData: ReplaceIcebergData): LogicalPlan = {
    val updatedMergeRows = replaceData.query match {
      case mergeRows@MergeRows(_, _, _, matchedOutputs, _, notMatchedOutputs, _, _, _, _, _) => {
        // ToDo check against more than just the name, cehck that it's a metadata attribute
        val rowIdOrdinal = matchedOutputs(0)(0)
          .indexWhere(attr => attr.isInstanceOf[Attribute] && attr.asInstanceOf[Attribute].name == "_row_id")
        val lastUpdatedOrdinal = matchedOutputs(0)(0)
          .indexWhere(attr => attr.isInstanceOf[Attribute]
            && attr.asInstanceOf[Attribute].name == "_last_updated_sequence_number")
        if (rowIdOrdinal == -1 || lastUpdatedOrdinal == -1) {
          replaceData.query
        } else {
          // ToDo more defensive here on expectations
          val updatedMatchOutputs =
            Seq(
              Seq(matchedOutputs(0)(0).updated(rowIdOrdinal,
                matchedOutputs(0)(0)(rowIdOrdinal)).updated(lastUpdatedOrdinal, Literal(null)))
            )
          val updatedNotMatchedOutputs = if (notMatchedOutputs.nonEmpty) {
            Seq(notMatchedOutputs(0)
              .updated(rowIdOrdinal, Literal(null)).updated(lastUpdatedOrdinal, Literal(null)))
          } else {
            notMatchedOutputs
          }

          val updatedMergeRows = mergeRows.copy(
            matchedOutputs = updatedMatchOutputs,
            notMatchedOutputs = updatedNotMatchedOutputs)
          updatedMergeRows
        }
      }
      case p => throw new UnsupportedOperationException(s"Unsupported $p")
    }
    val rowLineageAttributes = replaceData.originalTable.metadataOutput.filter(attr => attr.name == "_row_id"
      || attr.name == "_last_updated_sequence_number")

    ReplaceIcebergData(
      replaceData.table.asInstanceOf[DataSourceV2Relation]
        .copy(output = replaceData.table.asInstanceOf[DataSourceV2Relation].output
          ++ rowLineageAttributes.map(attr => attr.asInstanceOf[AttributeReference])),
      updatedMergeRows,
      replaceData.originalTable,
      replaceData.write)
  }

  private def rowLineageMetadataAttributes(metadataAttributes: Seq[Attribute]): Seq[Attribute] = {
    metadataAttributes.filter(attr => ROW_LINEAGE_ATTRIBUTES.contains(attr.name))
  }
}
