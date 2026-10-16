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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mumbling.MumblingTestUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestV4MetadataConversions {
  private static final FileIO IO = new InMemoryFileIO();
  private static final String TABLE_LOCATION = "s3://bucket/table";
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final TableMetadata PARTITIONED =
      TableMetadata.newTableMetadata(
          SCHEMA,
          PartitionSpec.builderFor(SCHEMA).identity("data").build(),
          SortOrder.unsorted(),
          TABLE_LOCATION,
          ImmutableMap.of(),
          4);
  private static final TableMetadata UNPARTITIONED =
      TableMetadata.newTableMetadata(
          SCHEMA,
          PartitionSpec.unpartitioned(),
          SortOrder.unsorted(),
          TABLE_LOCATION,
          ImmutableMap.of(),
          4);
  private static final long SNAPSHOT_ID = 100L;
  private static final long SEQUENCE_NUMBER = 7L;

  @Test
  void dataFileToTrackedFile() throws IOException {
    PartitionSpec spec = PARTITIONED.spec();
    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath(TABLE_LOCATION + "/data/a.parquet")
            .withFileSizeInBytes(100L)
            .withPartitionPath("data=a")
            .withMetrics(
                new Metrics(
                    10L,
                    null,
                    ImmutableMap.of(1, 10L),
                    ImmutableMap.of(1, 0L),
                    null,
                    ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 3)),
                    ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 9))))
            .withSplitOffsets(ImmutableList.of(4L))
            .build();

    ManifestFile leaf = writeRoot(PARTITIONED, writeLeaf(spec, dataFile)).get(0);
    TrackedFile file = Iterables.getOnlyElement(readLeaf(PARTITIONED, leaf));

    assertThat(file.contentType()).isEqualTo(FileContent.DATA);
    assertThat(file.location()).isEqualTo(dataFile.location());
    assertThat(file.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(file.recordCount()).isEqualTo(10L);
    assertThat(file.fileSizeInBytes()).isEqualTo(100L);
    assertThat(file.specId()).isEqualTo(spec.specId());
    assertThat(file.partition().get(0, CharSequence.class)).hasToString("a");
    assertThat(file.splitOffsets()).containsExactly(4L);
    assertThat(file.deletionVector()).isNull();
    assertThat(file.manifestInfo()).isNull();

    Tracking tracking = file.tracking();
    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(tracking.firstRowId()).isEqualTo(PARTITIONED.nextRowId());

    FieldStats<Integer> idStats = file.contentStats().statsFor(1);
    assertThat(idStats.lowerBound()).isEqualTo(3);
    assertThat(idStats.upperBound()).isEqualTo(9);
    assertThat(idStats.valueCount()).isEqualTo(10L);
    assertThat(idStats.nullValueCount()).isEqualTo(0L);
    assertThat(file.contentStats().statsFor(2)).isNull();
  }

  @Test
  void unpartitionedDataFileHasNoSpecId() throws IOException {
    DataFile dataFile =
        DataFiles.builder(UNPARTITIONED.spec())
            .withPath(TABLE_LOCATION + "/data/a.parquet")
            .withFileSizeInBytes(100L)
            .withRecordCount(10L)
            .build();

    ManifestFile leaf = writeRoot(UNPARTITIONED, writeLeaf(UNPARTITIONED.spec(), dataFile)).get(0);
    TrackedFile file = Iterables.getOnlyElement(readLeaf(UNPARTITIONED, leaf));

    assertThat(file.specId()).isNull();
    assertThat(file.partition()).isNull();
  }

  @Test
  void manifestToRootEntry() throws IOException {
    PartitionSpec spec = PARTITIONED.spec();
    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath(TABLE_LOCATION + "/data/a.parquet")
            .withFileSizeInBytes(100L)
            .withPartitionPath("data=a")
            .withRecordCount(10L)
            .build();
    ManifestFile leaf = writeLeaf(spec, dataFile);

    TrackedFile entry = rootEntry(writeRoot(PARTITIONED, leaf).get(0));

    assertThat(entry.contentType()).isEqualTo(FileContent.DATA_MANIFEST);
    assertThat(entry.location()).isEqualTo(leaf.path());
    assertThat(entry.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(entry.fileSizeInBytes()).isEqualTo(leaf.length());
    assertThat(entry.specId()).isEqualTo(spec.specId());
    assertThat(entry.recordCount()).isEqualTo(1L);

    Tracking tracking = entry.tracking();
    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(tracking.firstRowId()).isEqualTo(PARTITIONED.nextRowId());

    ManifestInfo info = entry.manifestInfo();
    assertThat(info.formatVersion()).isEqualTo(4);
    assertThat(info.addedFilesCount()).isEqualTo(1);
    assertThat(info.existingFilesCount()).isZero();
    assertThat(info.deletedFilesCount()).isZero();
    assertThat(info.replacedFilesCount()).isZero();
    assertThat(info.modifiedFilesCount()).isZero();
    assertThat(info.addedRowsCount()).isEqualTo(10L);
    assertThat(info.existingRowsCount()).isZero();
    assertThat(info.deletedRowsCount()).isZero();
    assertThat(info.replacedRowsCount()).isZero();
    assertThat(info.modifiedRowsCount()).isZero();
    assertThat(info.minSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(info.manifestDeletionVector()).isNull();
  }

  @Test
  void rootEntryLocationIsRelative() throws IOException {
    DataFile dataFile =
        DataFiles.builder(UNPARTITIONED.spec())
            .withPath(TABLE_LOCATION + "/data/a.parquet")
            .withFileSizeInBytes(100L)
            .withRecordCount(10L)
            .build();
    ManifestFile leaf = writeLeaf(UNPARTITIONED.spec(), dataFile);
    String rootLocation = writeRootLocation(UNPARTITIONED, leaf);

    String movedLocation = "s3://other-bucket/table";
    List<ManifestFile> manifests = readRoot(rootLocation, movedLocation);

    assertThat(manifests.get(0).path())
        .isEqualTo(movedLocation + leaf.path().substring(TABLE_LOCATION.length()));
  }

  @Test
  void preV4ManifestToRootEntry() throws IOException {
    DataFile dataFile =
        DataFiles.builder(UNPARTITIONED.spec())
            .withPath(TABLE_LOCATION + "/data/a.parquet")
            .withFileSizeInBytes(100L)
            .withRecordCount(10L)
            .build();
    String location = TABLE_LOCATION + "/metadata/" + FileFormat.AVRO.addExtension("v3-leaf");
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(3, UNPARTITIONED.spec(), IO.newOutputFile(location), SNAPSHOT_ID);
    try (writer) {
      writer.add(dataFile);
    }

    TrackedFile entry = rootEntry(writeRoot(UNPARTITIONED, writer.toManifestFile()).get(0));

    assertThat(entry.fileFormat()).isEqualTo(FileFormat.AVRO);
    assertThat(entry.specId()).isNull();
    assertThat(entry.manifestInfo().formatVersion()).isEqualTo(0);
  }

  @Test
  void existingManifestToRootEntry() throws IOException {
    long newSnapshotId = SNAPSHOT_ID + 1;
    ByteBuffer dv = MumblingTestUtil.build(MumblingTestUtil.sparse(1));
    ManifestInfo info =
        ManifestInfoStruct.builder()
            .addedFilesCount(1)
            .existingFilesCount(1)
            .deletedFilesCount(0)
            .replacedFilesCount(0)
            .modifiedFilesCount(0)
            .addedRowsCount(10L)
            .existingRowsCount(10L)
            .deletedRowsCount(0L)
            .replacedRowsCount(0L)
            .modifiedRowsCount(0L)
            .minSequenceNumber(SEQUENCE_NUMBER)
            .dv(dv)
            .formatVersion(4)
            .build();
    TrackedFile parentEntry =
        new TrackedFileStruct(
            new TrackingStruct(
                EntryStatus.ADDED,
                SNAPSHOT_ID,
                SEQUENCE_NUMBER,
                SEQUENCE_NUMBER,
                null,
                50L,
                null,
                null),
            FileContent.DATA_MANIFEST,
            TABLE_LOCATION + "/metadata/leaf.parquet",
            FileFormat.PARQUET,
            2L,
            1024L,
            null,
            null,
            null,
            null,
            null,
            info,
            null,
            null);

    String rootLocation = TABLE_LOCATION + "/metadata/" + FileFormat.PARQUET.addExtension("root");
    RootManifestWriter writer =
        new RootManifestWriter(
            IO.newOutputFile(rootLocation),
            UNPARTITIONED,
            MetricsConfig.getDefault(),
            newSnapshotId,
            SEQUENCE_NUMBER + 1);
    try (writer) {
      writer.add(TrackedFileAdapters.asManifestFile(parentEntry));
    }

    assertThat(writer.nextRowId()).isEqualTo(UNPARTITIONED.nextRowId());

    TrackedFile entry = rootEntry(readRoot(rootLocation, TABLE_LOCATION).get(0));
    assertThat(entry.location()).isEqualTo(parentEntry.location());
    assertThat(entry.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(entry.tracking().snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(entry.tracking().dataSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(entry.tracking().fileSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(entry.tracking().firstRowId()).isEqualTo(50L);
    assertThat(entry.manifestInfo().existingFilesCount()).isEqualTo(1);
    assertThat(entry.manifestInfo().manifestDeletionVector().isSet(1)).isTrue();
    assertThat(entry.manifestInfo().manifestDeletionVector().isSet(0)).isFalse();
  }

  @Test
  void existingDataFileToRootEntry() throws IOException {
    PartitionSpec spec = PARTITIONED.spec();
    PartitionData partition = new PartitionData(spec.partitionType());
    partition.set(0, "a");
    DeletionVector dv =
        DeletionVectorStruct.builder()
            .location(TABLE_LOCATION + "/data/dv.puffin")
            .offset(4L)
            .sizeInBytes(40L)
            .cardinality(2L)
            .build();
    TrackedFile parentFile =
        new TrackedFileStruct(
            new TrackingStruct(
                EntryStatus.ADDED,
                SNAPSHOT_ID,
                SEQUENCE_NUMBER,
                SEQUENCE_NUMBER,
                null,
                50L,
                null,
                null),
            FileContent.DATA,
            TABLE_LOCATION + "/data/a.parquet",
            FileFormat.PARQUET,
            10L,
            100L,
            spec.specId(),
            partition,
            null,
            null,
            dv,
            null,
            null,
            null);

    String rootLocation = TABLE_LOCATION + "/metadata/" + FileFormat.PARQUET.addExtension("root");
    try (RootManifestWriter writer =
        new RootManifestWriter(
            IO.newOutputFile(rootLocation),
            PARTITIONED,
            MetricsConfig.getDefault(),
            SNAPSHOT_ID + 1,
            SEQUENCE_NUMBER + 1)) {
      writer.addExisting(parentFile);
    }

    ManifestFile rootManifest = V4Metadata.rootManifest(snapshot(rootLocation, TABLE_LOCATION), IO);
    List<TrackedFile> files;
    try (V4ManifestReader reader =
        V4ManifestReader.builder(rootManifest, IO, PARTITIONED.schema(), PARTITIONED.specsById())
            .tableLocation(TABLE_LOCATION)
            .forScanPlanning()
            .build()) {
      files = Lists.newArrayList(reader);
    }

    TrackedFile file = Iterables.getOnlyElement(files);
    assertThat(file.location()).isEqualTo(parentFile.location());
    assertThat(file.specId()).isEqualTo(spec.specId());
    assertThat(file.partition().get(0, CharSequence.class)).hasToString("a");
    assertThat(file.deletionVector().location()).isEqualTo(dv.location());
    assertThat(file.deletionVector().cardinality()).isEqualTo(2L);
    assertThat(file.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(file.tracking().snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(file.tracking().dataSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(file.tracking().firstRowId()).isEqualTo(50L);
  }

  private static ManifestFile writeLeaf(PartitionSpec spec, DataFile... files) throws IOException {
    String location =
        TABLE_LOCATION
            + "/metadata/"
            + FileFormat.PARQUET.addExtension("leaf-" + System.nanoTime());
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(4, spec, IO.newOutputFile(location), SNAPSHOT_ID);
    try (writer) {
      for (DataFile file : files) {
        writer.add(file);
      }
    }

    return writer.toManifestFile();
  }

  private static List<ManifestFile> writeRoot(TableMetadata base, ManifestFile... manifests)
      throws IOException {
    return readRoot(writeRootLocation(base, manifests), TABLE_LOCATION);
  }

  private static String writeRootLocation(TableMetadata base, ManifestFile... manifests)
      throws IOException {
    String location =
        TABLE_LOCATION
            + "/metadata/"
            + FileFormat.PARQUET.addExtension("root-" + System.nanoTime());
    try (RootManifestWriter writer =
        new RootManifestWriter(
            IO.newOutputFile(location),
            base,
            MetricsConfig.getDefault(),
            SNAPSHOT_ID,
            SEQUENCE_NUMBER)) {
      for (ManifestFile manifest : manifests) {
        writer.add(manifest);
      }
    }

    return location;
  }

  private static List<ManifestFile> readRoot(String rootLocation, String tableLocation) {
    return snapshot(rootLocation, tableLocation).allManifests(IO);
  }

  private static Snapshot snapshot(String rootLocation, String tableLocation) {
    return new BaseSnapshot(
        SEQUENCE_NUMBER,
        SNAPSHOT_ID,
        null,
        System.currentTimeMillis(),
        DataOperations.APPEND,
        null,
        SCHEMA.schemaId(),
        rootLocation,
        0L,
        0L,
        null,
        tableLocation);
  }

  private static TrackedFile rootEntry(ManifestFile manifest) {
    return TrackedFileAdapters.trackedFile(manifest);
  }

  private static List<TrackedFile> readLeaf(TableMetadata base, ManifestFile leaf)
      throws IOException {
    try (V4ManifestReader reader =
        V4ManifestReader.builder(leaf, IO, base.schema(), base.specsById())
            .tableLocation(TABLE_LOCATION)
            .forScanPlanning()
            .projectStats(1, 2)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
