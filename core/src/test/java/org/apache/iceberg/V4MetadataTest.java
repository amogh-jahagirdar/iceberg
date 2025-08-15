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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V4MetadataTest {
  Random random = new Random(System.currentTimeMillis());

  @BeforeEach
  void setUp() {}

  @AfterEach
  void tearDown() {}

  @Test
  public void statsGen() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "s", Types.StringType.get()), optional(2, "i", Types.IntegerType.get()));

    Map<Integer, Supplier<Object>> statsSupplier =
        Map.of(
            1, () -> randomOf(List.of("Create", "Update", "Delete")),
            2, () -> randomOf(List.of(1, 2, 3)));

    V4MetadataSimbenchUtil simbenchUtil = new V4MetadataSimbenchUtil(schema, statsSupplier);

    try (V4Metadata2.Writer writer =
        new V4Metadata2.Writer(
            Files.localOutput("/tmp/v4/file.parquet"), FileFormat.PARQUET, schema, 0, 1L, 0, 0L)) {

      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withFormat(FileFormat.PARQUET)
              .withPath(String.format("00000-%s-%s-0-0000%s.parquet", 0, UUID.randomUUID(), 0))
              .withFileSizeInBytes(1024)
              .withRecordCount(1)
              .build();

      V4ManifestEntry.EntryV4 entry = new V4ManifestEntry.EntryV4();
      entry
          .status(ManifestEntry.Status.ADDED)
          .snapshotId(1L)
          .sequenceNumber(1L)
          .content()
          .fileFormat(FileFormat.PARQUET)
          .contentStats(simbenchUtil.generateContentStats(schema, statsSupplier));

      writer.add(entry);
    }
  }

  @Test
  public void manifestSchema() {
    Schema schema =
        new Schema(
            optional(1, "s", Types.StringType.get()), optional(2, "i", Types.IntegerType.get()));

    V4ContentEntry.ContentStatsSchemaVisitor visitor =
        new V4ContentEntry.ContentStatsSchemaVisitor();

    Types.NestedField statsField = TypeUtil.visit(schema, visitor);

    System.out.println(statsField);
  }

  @Test
  public void testWriteEntry() throws IOException {
    try (V4Metadata2.Writer writer =
        new V4Metadata2.Writer(
            Files.localOutput("/tmp/v4/file.avro"), FileFormat.AVRO, new Schema(), 0, 1L, 0, 0L)) {

      V4ManifestEntry.EntryV4 entry = new V4ManifestEntry.EntryV4();
      entry
          .status(ManifestEntry.Status.ADDED)
          .snapshotId(1L)
          .sequenceNumber(1L)
          .content()
          .location(String.format("00000-%s-%s-0-0000%s.parquet", 0, UUID.randomUUID(), 0))
          .fileFormat(FileFormat.PARQUET)
          .fileSize(1024L)
          .recordCount(1L);

      writer.add(entry);
    }
  }

  private Object randomOf(List<?> values) {
    return values.get(random.nextInt(values.size()));
  }
}
