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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mumbling.MumblingTestUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestManifestExpander {
  private static final FileIO IO = new InMemoryFileIO();
  private static final String TABLE_LOCATION = "s3://bucket/table";
  private static final TableMetadata BASE =
      TableMetadata.newTableMetadata(
          new Schema(required(1, "id", Types.LongType.get())),
          PartitionSpec.unpartitioned(),
          SortOrder.unsorted(),
          TABLE_LOCATION,
          ImmutableMap.of(),
          4);
  private static final long SNAPSHOT_ID = 100L;
  private static final long SEQUENCE_NUMBER = 1L;

  @Test
  void plansFilesFromLeafManifests() throws IOException {
    ManifestFile first = writeLeaf("a.parquet");
    ManifestFile second = writeLeaf("b.parquet", "c.parquet");

    List<FileScanTask> tasks =
        planFiles(
            writeRoot(
                writer -> {
                  writer.add(first);
                  writer.add(second);
                }));

    assertThat(locations(tasks))
        .containsExactlyInAnyOrder(
            dataLocation("a.parquet"), dataLocation("b.parquet"), dataLocation("c.parquet"));
    assertThat(tasks).allSatisfy(task -> assertThat(task.deletes()).isEmpty());
  }

  @Test
  void skipsPositionsInManifestDeletionVector() throws IOException {
    ManifestFile leaf = writeLeaf("a.parquet", "b.parquet", "c.parquet");
    ManifestInfo info =
        ManifestInfoStruct.builder()
            .addedFilesCount(3)
            .existingFilesCount(0)
            .deletedFilesCount(0)
            .replacedFilesCount(0)
            .modifiedFilesCount(0)
            .addedRowsCount(30L)
            .existingRowsCount(0L)
            .deletedRowsCount(0L)
            .replacedRowsCount(0L)
            .modifiedRowsCount(0L)
            .minSequenceNumber(SEQUENCE_NUMBER)
            .dv(MumblingTestUtil.build(MumblingTestUtil.sparse(0, 2)))
            .formatVersion(4)
            .build();
    TrackedFile leafWithDV =
        new TrackedFileStruct(
            tracking(0L),
            FileContent.DATA_MANIFEST,
            leaf.path(),
            FileFormat.PARQUET,
            3L,
            leaf.length(),
            null,
            null,
            null,
            null,
            null,
            info,
            null,
            null);

    List<FileScanTask> tasks =
        planFiles(writeRoot(writer -> writer.add(TrackedFileAdapters.asManifestFile(leafWithDV))));

    assertThat(locations(tasks)).containsExactly(dataLocation("b.parquet"));
  }

  @Test
  void plansDataFilesInRootManifest() throws IOException {
    ManifestFile leaf = writeLeaf("a.parquet");
    TrackedFile rootDataFile = rootDataFile("b.parquet", null);

    List<FileScanTask> tasks =
        planFiles(
            writeRoot(
                writer -> {
                  writer.add(leaf);
                  writer.addExisting(rootDataFile);
                }));

    assertThat(locations(tasks))
        .containsExactlyInAnyOrder(dataLocation("a.parquet"), dataLocation("b.parquet"));
  }

  @Test
  void attachesDeletionVector() throws IOException {
    DeletionVector dv =
        DeletionVectorStruct.builder()
            .location(TABLE_LOCATION + "/data/dv.puffin")
            .offset(4L)
            .sizeInBytes(40L)
            .cardinality(2L)
            .build();
    TrackedFile rootDataFile = rootDataFile("a.parquet", dv);

    List<FileScanTask> tasks = planFiles(writeRoot(writer -> writer.addExisting(rootDataFile)));

    assertThat(tasks).hasSize(1);
    assertThat(tasks.get(0).deletes()).hasSize(1);
    DeleteFile delete = tasks.get(0).deletes().get(0);
    assertThat(delete.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(delete.location()).isEqualTo(dv.location());
    assertThat(delete.referencedDataFile()).isEqualTo(dataLocation("a.parquet"));
    assertThat(delete.contentOffset()).isEqualTo(4L);
    assertThat(delete.contentSizeInBytes()).isEqualTo(40L);
    assertThat(delete.recordCount()).isEqualTo(2L);
  }

  private static String dataLocation(String fileName) {
    return TABLE_LOCATION + "/data/" + fileName;
  }

  private static Tracking tracking(Long firstRowId) {
    return new TrackingStruct(
        EntryStatus.ADDED,
        SNAPSHOT_ID,
        SEQUENCE_NUMBER,
        SEQUENCE_NUMBER,
        null,
        firstRowId,
        null,
        null);
  }

  private static TrackedFile rootDataFile(String fileName, DeletionVector dv) {
    return new TrackedFileStruct(
        tracking(100L),
        FileContent.DATA,
        dataLocation(fileName),
        FileFormat.PARQUET,
        10L,
        1024L,
        null,
        null,
        null,
        null,
        dv,
        null,
        null,
        null);
  }

  private static ManifestFile writeLeaf(String... fileNames) throws IOException {
    String location =
        TABLE_LOCATION
            + "/metadata/"
            + FileFormat.PARQUET.addExtension("leaf-" + System.nanoTime());
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(4, BASE.spec(), IO.newOutputFile(location), SNAPSHOT_ID);
    try (writer) {
      for (String fileName : fileNames) {
        writer.add(
            DataFiles.builder(BASE.spec())
                .withPath(dataLocation(fileName))
                .withFileSizeInBytes(1024L)
                .withRecordCount(10L)
                .build());
      }
    }

    return writer.toManifestFile();
  }

  private static Snapshot writeRoot(Consumer<RootManifestWriter> addEntries) throws IOException {
    String location =
        TABLE_LOCATION
            + "/metadata/"
            + FileFormat.PARQUET.addExtension("root-" + System.nanoTime());
    try (RootManifestWriter writer =
        new RootManifestWriter(
            IO.newOutputFile(location),
            BASE,
            MetricsConfig.getDefault(),
            SNAPSHOT_ID,
            SEQUENCE_NUMBER)) {
      addEntries.accept(writer);
    }

    return new BaseSnapshot(
        SEQUENCE_NUMBER,
        SNAPSHOT_ID,
        null,
        System.currentTimeMillis(),
        DataOperations.APPEND,
        null,
        BASE.currentSchemaId(),
        location,
        0L,
        0L,
        null,
        TABLE_LOCATION);
  }

  private static List<FileScanTask> planFiles(Snapshot snapshot) throws IOException {
    ManifestExpander expander =
        new ManifestExpander(
            IO,
            V4Metadata.rootManifest(snapshot, IO),
            BASE.schema(),
            BASE.specsById(),
            TABLE_LOCATION);
    try (CloseableIterable<FileScanTask> tasks = expander.planFiles()) {
      return Lists.newArrayList(tasks);
    }
  }

  private static List<String> locations(List<FileScanTask> tasks) {
    return Lists.transform(tasks, task -> task.file().location());
  }
}
