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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;

/** Writer for the root manifest of a v4 snapshot. */
class RootManifestWriter implements Closeable {
  private final FileAppender<StructLike> writer;
  private final long snapshotId;
  private final long sequenceNumber;
  private final String tableLocation;
  private final Map<Integer, PartitionSpec> specsById;
  private final Types.StructType partitionType;
  private long nextRowId;

  RootManifestWriter(
      OutputFile file,
      TableMetadata base,
      MetricsConfig metricsConfig,
      long snapshotId,
      long sequenceNumber) {
    this.snapshotId = snapshotId;
    this.sequenceNumber = sequenceNumber;
    this.tableLocation = base.location();
    this.specsById = base.specsById();
    this.partitionType = Partitioning.unionPartitionTypes(base.specs());
    this.nextRowId = base.nextRowId();
    Types.StructType statsType = StatsUtil.statsWriteSchema(base.schema(), metricsConfig);
    this.writer =
        newAppender(file, TrackedFile.schema(partitionType, statsType), base.currentSchemaId());
  }

  void add(ManifestFile manifest) {
    TrackedFile rootEntry = TrackedFileAdapters.trackedFile(manifest);
    if (rootEntry != null) {
      writer.add(existingManifest(rootEntry));
    } else {
      writer.add(manifestEntry(manifest));
    }
  }

  /** Adds a live data file from the root manifest of the parent snapshot. */
  void addExisting(TrackedFile dataFile) {
    writer.add(existingDataFile(dataFile));
  }

  /** Returns the next row ID that is not assigned by this writer. */
  long nextRowId() {
    return nextRowId;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  private TrackedFileStruct manifestEntry(ManifestFile manifest) {
    long manifestSequenceNumber = assigned(manifest.sequenceNumber());
    Tracking tracking =
        new TrackingStruct(
            manifest.snapshotId() == snapshotId ? EntryStatus.ADDED : EntryStatus.EXISTING,
            manifest.snapshotId(),
            manifestSequenceNumber,
            manifestSequenceNumber,
            null /* modified snapshot ID */,
            firstRowId(manifest),
            null /* deleted positions */,
            null /* replaced positions */);

    ManifestInfo info =
        ManifestInfoStruct.builder()
            .addedFilesCount(manifest.addedFilesCount())
            .existingFilesCount(manifest.existingFilesCount())
            .deletedFilesCount(manifest.deletedFilesCount())
            .replacedFilesCount(0)
            .modifiedFilesCount(0)
            .addedRowsCount(manifest.addedRowsCount())
            .existingRowsCount(manifest.existingRowsCount())
            .deletedRowsCount(manifest.deletedRowsCount())
            .replacedRowsCount(0L)
            .modifiedRowsCount(0L)
            .minSequenceNumber(assigned(manifest.minSequenceNumber()))
            .formatVersion(manifest.formatVersion())
            .build();

    return new TrackedFileStruct(
        tracking,
        manifest.content() == ManifestContent.DATA
            ? FileContent.DATA_MANIFEST
            : FileContent.DELETE_MANIFEST,
        LocationUtil.relativizeLocation(tableLocation, manifest.path()),
        FileFormat.fromFileName(manifest.path()),
        recordCount(info),
        manifest.length(),
        specId(manifest),
        null /* partition */,
        null /* content stats */,
        null /* sort order ID */,
        null /* deletion vector */,
        info,
        manifest.keyMetadata(),
        null /* split offsets */);
  }

  private TrackedFileStruct existingManifest(TrackedFile manifest) {
    // the reader assigns first row IDs to delete manifests, and delete manifests must not have one
    Long firstRowId =
        manifest.contentType() == FileContent.DATA_MANIFEST
            ? manifest.tracking().firstRowId()
            : null;
    return new TrackedFileStruct(
        existing(manifest.tracking(), firstRowId),
        manifest.contentType(),
        LocationUtil.relativizeLocation(tableLocation, manifest.location()),
        manifest.fileFormat(),
        manifest.recordCount(),
        manifest.fileSizeInBytes(),
        manifest.specId(),
        null /* partition */,
        null /* content stats */,
        null /* sort order ID */,
        null /* deletion vector */,
        manifest.manifestInfo(),
        manifest.keyMetadata(),
        null /* split offsets */);
  }

  private TrackedFileStruct existingDataFile(TrackedFile file) {
    return new TrackedFileStruct(
        existing(file.tracking(), file.tracking().firstRowId()),
        FileContent.DATA,
        LocationUtil.relativizeLocation(tableLocation, file.location()),
        file.fileFormat(),
        file.recordCount(),
        file.fileSizeInBytes(),
        file.specId(),
        unionPartition(file),
        file.contentStats(),
        file.sortOrderId(),
        relativize(file.deletionVector()),
        null /* manifest info */,
        file.keyMetadata(),
        file.splitOffsets());
  }

  private static Tracking existing(Tracking tracking, Long firstRowId) {
    return new TrackingStruct(
        EntryStatus.EXISTING,
        tracking.snapshotId(),
        tracking.dataSequenceNumber(),
        tracking.fileSequenceNumber(),
        tracking.modifiedSnapshotId(),
        firstRowId,
        null /* deleted positions */,
        null /* replaced positions */);
  }

  private long assigned(long manifestSequenceNumber) {
    return manifestSequenceNumber == ManifestWriter.UNASSIGNED_SEQ
        ? sequenceNumber
        : manifestSequenceNumber;
  }

  private Long firstRowId(ManifestFile manifest) {
    if (manifest.content() != ManifestContent.DATA || manifest.firstRowId() != null) {
      return manifest.firstRowId();
    }

    long firstRowId = nextRowId;
    // include existing rows because files from pre-v3 snapshots have no first row ID
    this.nextRowId += manifest.existingRowsCount() + manifest.addedRowsCount();
    return firstRowId;
  }

  private Integer specId(ManifestFile manifest) {
    PartitionSpec spec = specsById.get(manifest.partitionSpecId());
    return spec.isPartitioned() ? spec.specId() : null;
  }

  private static long recordCount(ManifestInfo info) {
    return (long) info.addedFilesCount()
        + info.existingFilesCount()
        + info.deletedFilesCount()
        + info.replacedFilesCount()
        + info.modifiedFilesCount();
  }

  // the reader returns the partition with the type of the file's spec
  private PartitionData unionPartition(TrackedFile file) {
    if (file.specId() == null) {
      return null;
    }

    StructLike partition = file.partition();
    List<Types.NestedField> fields = specsById.get(file.specId()).partitionType().fields();
    List<Types.NestedField> unionFields = partitionType.fields();
    PartitionData unionPartition = new PartitionData(partitionType);
    for (int pos = 0; pos < fields.size(); pos += 1) {
      Types.NestedField unionField = partitionType.field(fields.get(pos).fieldId());
      unionPartition.set(unionFields.indexOf(unionField), partition.get(pos, Object.class));
    }

    return unionPartition;
  }

  private DeletionVector relativize(DeletionVector dv) {
    if (dv == null) {
      return null;
    }

    return DeletionVectorStruct.builder()
        .location(LocationUtil.relativizeLocation(tableLocation, dv.location()))
        .offset(dv.offset())
        .sizeInBytes(dv.sizeInBytes())
        .cardinality(dv.cardinality())
        .keyMetadata(dv.keyMetadata())
        .build();
  }

  private static FileAppender<StructLike> newAppender(
      OutputFile file, Schema schema, int schemaId) {
    try {
      return InternalData.write(FileFormat.PARQUET, file)
          .schema(schema)
          .named("tracked_file")
          .meta("schema-id", String.valueOf(schemaId))
          .meta("format-version", String.valueOf(V4Metadata.FORMAT_VERSION))
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to create root manifest writer for path: %s", file.location());
    }
  }
}
