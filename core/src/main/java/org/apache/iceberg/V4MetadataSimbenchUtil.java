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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class V4MetadataSimbenchUtil {
  private Random random = new Random();

  private Schema schema;
  private Map<Integer, Supplier<Object>> statsSupplier;

  V4MetadataSimbenchUtil(Schema schema, Map<Integer, Supplier<Object>> statsSupplier) {
    this.schema = schema;
    this.statsSupplier = statsSupplier;
  }

  V4ManifestEntry.EntryV4 generateManifest(
      Collection<V4ManifestEntry> entries, long sequenceNumber, OutputFile manifestOutputFile) {
    long minSequenceNumber = Long.MAX_VALUE;
    int addedFileCount = 0;
    int existingFileCount = 0;
    int deletedFileCount = 0;
    long addedRowsCount = 0;
    long existingRowsCount = 0;
    long deletedRowsCount = 0;

    for (V4ManifestEntry entry : entries) {
      switch (entry.status()) {
        case ADDED:
          addedFileCount++;
          addedRowsCount += entry.content().recordCount();
          minSequenceNumber = Math.min(minSequenceNumber, entry.sequenceNumber());
          break;
        case EXISTING:
          existingFileCount++;
          existingRowsCount += entry.content().recordCount();
          minSequenceNumber = Math.min(minSequenceNumber, entry.sequenceNumber());
          break;
        case DELETED:
          deletedFileCount++;
          deletedRowsCount += entry.content().recordCount();
          minSequenceNumber = Math.min(minSequenceNumber, entry.sequenceNumber());
          break;
      }
    }

    try (V4Metadata2.Writer writer =
        new V4Metadata2.Writer(
            manifestOutputFile,
            FileFormat.PARQUET,
            schema,
            sequenceNumber,
            sequenceNumber - 1,
            sequenceNumber,
            0L)) {
      entries.forEach(writer::add);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    V4ManifestEntry.EntryV4 result = new V4ManifestEntry.EntryV4();
    result
        .minSequenceNumber(sequenceNumber)
        .contentType(ManifestContent.DATA)
        .status(ManifestEntry.Status.ADDED)
        .snapshotId(sequenceNumber)
        .sequenceNumber(sequenceNumber)
        .content()
        .location(manifestOutputFile.location())
        .fileFormat(FileFormat.PARQUET)
        .recordCount(addedRowsCount + existingRowsCount)
        .fileSize(manifestOutputFile.toInputFile().getLength())
        .minSequenceNumber(sequenceNumber)
        .partitionSpecId(0)
        // Note: Just reuse some stats to populate the fields
        .contentStats(entries.stream().findFirst().get().content().contentStats())
        .manifestStats(
            new V4ContentEntry.ManifestStats(
                addedFileCount,
                existingFileCount,
                deletedFileCount,
                addedRowsCount,
                existingRowsCount,
                deletedRowsCount));

    return result;
  }

  public V4ManifestEntry.EntryV4 generateDataFileEntry(
      long sequenceNumber, String writeUUID, int taskId) {
    V4ManifestEntry.EntryV4 entry = new V4ManifestEntry.EntryV4();
    entry
        .status(ManifestEntry.Status.ADDED)
        .contentType(ManifestContent.DATA)
        .snapshotId(sequenceNumber)
        .sequenceNumber(sequenceNumber)
        .content()
        .fileFormat(FileFormat.PARQUET)
        .location(String.format("00000-%s-%s-0000-%s.parquet", sequenceNumber, writeUUID, taskId))
        .fileSize((long) ThreadLocalRandom.current().nextInt(8 * 1024 * 1024, 128 * 1024 * 1024))
        .minSequenceNumber(sequenceNumber)
        .recordCount((long) ThreadLocalRandom.current().nextInt(10_000, 100_000))
        .contentStats(generateContentStats(schema, statsSupplier))
        .offsets(List.of(0L));

    return entry;
  }

//  public V4ManifestEntry.EntryV4 generateInlineDv(long sequenceNumber, ) {
//    V4ManifestEntry.EntryV4 entry = new V4ManifestEntry.EntryV4();
//    entry
//            .status(ManifestEntry.Status.ADDED)
//            .contentType(ManifestContent.MANIFEST_DV)
//            .sequenceNumber(seq)
//            .
//  }

  public V4ContentEntry.ContentStats generateContentStats(
      Schema schema, Map<Integer, Supplier<Object>> statsSupplier) {
    return TypeUtil.visit(schema, new StatsGentVisitor(schema, statsSupplier));
  }

  private class StatsGentVisitor extends TypeUtil.SchemaVisitor<V4ContentEntry.ContentStats> {
    private final Map<Integer, Supplier<Object>> statsSupplier;
    private final V4ContentEntry.ContentStats contentStats;
    private final int rowCount = random.nextInt(100_000);

    public StatsGentVisitor(Schema schema, Map<Integer, Supplier<Object>> statsSupplier) {
      this.statsSupplier = statsSupplier;
      this.contentStats = new V4ContentEntry.ContentStats(schema.columns().size());
    }

    @Override
    public V4ContentEntry.ContentStats field(
        Types.NestedField field, V4ContentEntry.ContentStats fieldResult) {
      V4ContentEntry.FieldStats fieldStats =
          new V4ContentEntry.FieldStats()
              .upperBound(statsSupplier.get(field.fieldId()).get())
              .lowerBound(statsSupplier.get(field.fieldId()).get())
              .size(random.nextLong())
              .valueCount(rowCount)
              .nullValueCount(0)
              .recordCount(rowCount);

      contentStats.set(field.fieldId() - 1, fieldStats);

      return contentStats;
    }

    @Override
    public V4ContentEntry.ContentStats struct(
        Types.StructType struct, List<V4ContentEntry.ContentStats> fieldResults) {
      return contentStats;
    }

    @Override
    public V4ContentEntry.ContentStats schema(
        Schema schema, V4ContentEntry.ContentStats structResult) {
      return contentStats;
    }
  }
}
