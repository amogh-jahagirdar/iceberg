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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

class V4Metadata2 {

  static Types.NestedField STATUS = required(0, "status", Types.IntegerType.get());
  static Types.NestedField SNAPSHOT_ID = optional(1, "snapshot_id", Types.LongType.get());
  static Types.NestedField CONTENT_TYPE = required(134, "content_type", Types.IntegerType.get());
  static Types.NestedField DV_CONTENT = optional(146, "dv_content", Types.BinaryType.get());
  static Types.NestedField SEQUENCE_NUMBER = optional(3, "sequence_number", Types.LongType.get());
  static Types.NestedField MIN_SEQUENCE_NUMBER = optional(516, "min_sequence_number", Types.LongType.get());

  private V4Metadata2() {}

  static Schema manifestSchema(Schema dataSchema) {
    return new Schema(
        STATUS,
        SNAPSHOT_ID,
        CONTENT_TYPE,
        DV_CONTENT,
        V4ContentEntry.fieldFor(dataSchema),
        SEQUENCE_NUMBER,
        MIN_SEQUENCE_NUMBER
    );
  }

  static class Writer implements FileAppender<V4ManifestEntry> {
    FileAppender<V4ManifestEntry> appender;

    Writer(
        OutputFile file,
        FileFormat format,
        Schema tableSchema,
        long snapshotId,
        Long parentSnapshotId,
        long sequenceNumber,
        long firstRowId) {

      Map<String, String> meta =
          ImmutableMap.of(
              "snapshot-id", String.valueOf(snapshotId),
              "parent-snapshot-id", String.valueOf(parentSnapshotId),
              "sequence-number", String.valueOf(sequenceNumber),
              "first-row-id", String.valueOf(firstRowId),
              "format-version", "4");

      try {
        MetricsConfig metricsConfig =
            MetricsConfig.fromProperties(
                Map.of("write.metadata.metrics.max-inferred-column-defaults", "0"));

        InternalData.WriteBuilder writeBuilder =
            InternalData.write(format, file)
                .schema(manifestSchema(tableSchema))
                .named("manifest_file")
                .meta(meta)
                .metricsConfig(metricsConfig)
                .overwrite()
                .set("write.parquet.compression-codec", "zstd")
                .set("write.parquet.compression-level", "3")
                .set("parquet.column.statistics.enabled", "false")
                .set("write.parquet.column.statistics.enabled", "false")
                .set("write.metadata.metrics.max-inferred-column-defaults", "0");

        this.appender = writeBuilder.build();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void add(V4ManifestEntry entry) {
      this.appender.add(entry);
    }

    @Override
    public Metrics metrics() {
      return null;
    }

    @Override
    public long length() {
      return 0;
    }

    @Override
    public void close() throws IOException {
      appender.close();
    }
  }
}
