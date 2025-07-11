/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.procedures;

import java.util.Iterator;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that applies changes in a given snapshot and creates a new snapshot which will be set
 * as the current snapshot in a table.
 *
 * <p><em>Note:</em> this procedure invalidates all cached Spark plans that reference the affected
 * table.
 *
 * @see org.apache.iceberg.ManageSnapshots#cherrypick(long)
 */
class CherrypickSnapshotProcedure extends BaseProcedure {

  static final String NAME = "cherrypick_snapshot";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        requiredInParameter("table", DataTypes.StringType),
        requiredInParameter("snapshot_id", DataTypes.LongType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("source_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("current_snapshot_id", DataTypes.LongType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<CherrypickSnapshotProcedure>() {
      @Override
      protected CherrypickSnapshotProcedure doBuild() {
        return new CherrypickSnapshotProcedure(tableCatalog());
      }
    };
  }

  private CherrypickSnapshotProcedure(TableCatalog catalog) {
    super(catalog);
  }

  @Override
  public BoundProcedure bind(StructType inputType) {
    return this;
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public Iterator<Scan> call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    long snapshotId = args.getLong(1);

    return asScanIterator(
        OUTPUT_TYPE,
        modifyIcebergTable(
            tableIdent,
            table -> {
              table.manageSnapshots().cherrypick(snapshotId).commit();

              Snapshot currentSnapshot = table.currentSnapshot();

              InternalRow outputRow = newInternalRow(snapshotId, currentSnapshot.snapshotId());
              return new InternalRow[] {outputRow};
            }));
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "CherrypickSnapshotProcedure";
  }
}
