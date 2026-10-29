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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

class V4Metadata {
  private V4Metadata() {}

  static final int FORMAT_VERSION = 4;
  static final int PRE_V4_FORMAT_VERSION = 0;

  static final Schema MANIFEST_LIST_SCHEMA =
      new Schema(
          ManifestFile.PATH,
          ManifestFile.LENGTH,
          ManifestFile.SPEC_ID,
          ManifestFile.MANIFEST_CONTENT.asRequired(),
          ManifestFile.SEQUENCE_NUMBER.asRequired(),
          ManifestFile.MIN_SEQUENCE_NUMBER.asRequired(),
          ManifestFile.SNAPSHOT_ID,
          ManifestFile.ADDED_FILES_COUNT.asRequired(),
          ManifestFile.EXISTING_FILES_COUNT.asRequired(),
          ManifestFile.DELETED_FILES_COUNT.asRequired(),
          ManifestFile.ADDED_ROWS_COUNT.asRequired(),
          ManifestFile.EXISTING_ROWS_COUNT.asRequired(),
          ManifestFile.DELETED_ROWS_COUNT.asRequired(),
          ManifestFile.PARTITION_SUMMARIES,
          ManifestFile.KEY_METADATA,
          ManifestFile.FIRST_ROW_ID);

  /**
   * A wrapper class to write any ManifestFile implementation to Avro using the v4 write schema.
   *
   * <p>This is used to maintain compatibility with v4 by writing manifest list files with the old
   * schema, instead of writing a sequence number into metadata files in v4 tables.
   */
  static class ManifestFileWrapper implements ManifestFile, StructLike {
    private final long commitSnapshotId;
    private final long sequenceNumber;
    private ManifestFile wrapped = null;
    private Long wrappedFirstRowId = null;

    ManifestFileWrapper(long commitSnapshotId, long sequenceNumber) {
      this.commitSnapshotId = commitSnapshotId;
      this.sequenceNumber = sequenceNumber;
    }

    public ManifestFile wrap(ManifestFile file, Long firstRowId) {
      this.wrapped = file;
      this.wrappedFirstRowId = firstRowId;
      return this;
    }

    @Override
    public int size() {
      return MANIFEST_LIST_SCHEMA.columns().size();
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify ManifestFileWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      switch (pos) {
        case 0:
          return wrapped.path();
        case 1:
          return wrapped.length();
        case 2:
          return wrapped.partitionSpecId();
        case 3:
          return wrapped.content().id();
        case 4:
          if (wrapped.sequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
            // if the sequence number is being assigned here, then the manifest must be created by
            // the current
            // operation. to validate this, check that the snapshot id matches the current commit
            Preconditions.checkState(
                commitSnapshotId == wrapped.snapshotId(),
                "Found unassigned sequence number for a manifest from snapshot: %s",
                wrapped.snapshotId());
            return sequenceNumber;
          } else {
            return wrapped.sequenceNumber();
          }
        case 5:
          if (wrapped.minSequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
            // same sanity check as above
            Preconditions.checkState(
                commitSnapshotId == wrapped.snapshotId(),
                "Found unassigned sequence number for a manifest from snapshot: %s",
                wrapped.snapshotId());
            // if the min sequence number is not determined, then there was no assigned sequence
            // number for any file
            // written to the wrapped manifest. replace the unassigned sequence number with the one
            // for this commit
            return sequenceNumber;
          } else {
            return wrapped.minSequenceNumber();
          }
        case 6:
          return wrapped.snapshotId();
        case 7:
          return wrapped.addedFilesCount();
        case 8:
          return wrapped.existingFilesCount();
        case 9:
          return wrapped.deletedFilesCount();
        case 10:
          return wrapped.addedRowsCount();
        case 11:
          return wrapped.existingRowsCount();
        case 12:
          return wrapped.deletedRowsCount();
        case 13:
          return wrapped.partitions();
        case 14:
          return wrapped.keyMetadata();
        case 15:
          if (wrappedFirstRowId != null) {
            // if first-row-id is assigned, ensure that it is valid
            Preconditions.checkState(
                wrapped.content() == ManifestContent.DATA && wrapped.firstRowId() == null,
                "Found invalid first-row-id assignment: %s",
                wrapped);
            return wrappedFirstRowId;
          } else if (wrapped.content() != ManifestContent.DATA) {
            return null;
          } else {
            Preconditions.checkState(
                wrapped.firstRowId() != null,
                "Found unassigned first-row-id for file: " + wrapped.path());
            return wrapped.firstRowId();
          }
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public String path() {
      return wrapped.path();
    }

    @Override
    public long length() {
      return wrapped.length();
    }

    @Override
    public int partitionSpecId() {
      return wrapped.partitionSpecId();
    }

    @Override
    public ManifestContent content() {
      return wrapped.content();
    }

    @Override
    public long sequenceNumber() {
      return wrapped.sequenceNumber();
    }

    @Override
    public long minSequenceNumber() {
      return wrapped.minSequenceNumber();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public boolean hasAddedFiles() {
      return wrapped.hasAddedFiles();
    }

    @Override
    public Integer addedFilesCount() {
      return wrapped.addedFilesCount();
    }

    @Override
    public Long addedRowsCount() {
      return wrapped.addedRowsCount();
    }

    @Override
    public boolean hasExistingFiles() {
      return wrapped.hasExistingFiles();
    }

    @Override
    public Integer existingFilesCount() {
      return wrapped.existingFilesCount();
    }

    @Override
    public Long existingRowsCount() {
      return wrapped.existingRowsCount();
    }

    @Override
    public boolean hasDeletedFiles() {
      return wrapped.hasDeletedFiles();
    }

    @Override
    public Integer deletedFilesCount() {
      return wrapped.deletedFilesCount();
    }

    @Override
    public Long deletedRowsCount() {
      return wrapped.deletedRowsCount();
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return wrapped.partitions();
    }

    @Override
    public ByteBuffer keyMetadata() {
      return wrapped.keyMetadata();
    }

    @Override
    public Long firstRowId() {
      return wrapped.firstRowId();
    }

    @Override
    public ManifestFile copy() {
      return wrapped.copy();
    }
  }

  static Schema manifestSchema(PartitionSpec spec) {
    return TrackedFile.schema(spec.partitionType(), statsType(spec.schema()));
  }

  /** Returns the content stats type with full stats for every field in the schema. */
  static Types.StructType statsType(Schema schema) {
    return StatsUtil.statsReadSchema(schema, TypeUtil.getProjectedIds(schema));
  }

  static boolean hasRootManifest(Snapshot snapshot) {
    String location = snapshot.manifestListLocation();
    return location != null && FileFormat.fromFileName(location) == FileFormat.PARQUET;
  }

  /** Returns the root manifest of a snapshot as a manifest that can be read as a v4 manifest. */
  static ManifestFile rootManifest(Snapshot snapshot, FileIO io) {
    String location = snapshot.manifestListLocation();
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            snapshot.snapshotId(),
            snapshot.sequenceNumber(),
            snapshot.sequenceNumber(),
            null /* modified snapshot ID */,
            snapshot.firstRowId(),
            null /* deleted positions */,
            null /* replaced positions */);
    ManifestInfo info =
        new ManifestInfoStruct(
            0, 0, 0, 0, 0, 0L, 0L, 0L, 0L, 0L, snapshot.sequenceNumber(), null, FORMAT_VERSION);
    TrackedFile root =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA_MANIFEST,
            location,
            FileFormat.PARQUET,
            0L,
            io.newInputFile(location).getLength(),
            null /* spec ID */,
            null /* partition */,
            null /* content stats */,
            null /* sort order ID */,
            null /* deletion vector */,
            info,
            null /* key metadata */,
            null /* split offsets */);
    return TrackedFileAdapters.asManifestFile(root);
  }

  static Schema entrySchema(Types.StructType partitionType) {
    return wrapFileSchema(fileType(partitionType));
  }

  static Schema wrapFileSchema(Types.StructType fileSchema) {
    // this is used to build projection schemas
    return new Schema(
        ManifestEntry.STATUS,
        ManifestEntry.SNAPSHOT_ID,
        ManifestEntry.SEQUENCE_NUMBER,
        ManifestEntry.FILE_SEQUENCE_NUMBER,
        required(ManifestEntry.DATA_FILE_ID, "data_file", fileSchema));
  }

  static Types.StructType fileType(Types.StructType partitionType) {
    // Parquet cannot represent empty groups, so the partition field is omitted entirely from
    // the file schema for unpartitioned tables. DataFileWrapper adjusts positions to match.
    ImmutableList.Builder<Types.NestedField> fields =
        ImmutableList.builderWithExpectedSize(partitionType.fields().isEmpty() ? 18 : 19);
    fields.add(DataFile.CONTENT.asRequired());
    fields.add(DataFile.FILE_PATH);
    fields.add(DataFile.FILE_FORMAT);
    if (!partitionType.fields().isEmpty()) {
      fields.add(
          required(
              DataFile.PARTITION_ID,
              DataFile.PARTITION_NAME,
              partitionType,
              DataFile.PARTITION_DOC));
    }
    fields.add(DataFile.RECORD_COUNT);
    fields.add(DataFile.FILE_SIZE);
    fields.add(DataFile.COLUMN_SIZES);
    fields.add(DataFile.VALUE_COUNTS);
    fields.add(DataFile.NULL_VALUE_COUNTS);
    fields.add(DataFile.NAN_VALUE_COUNTS);
    fields.add(DataFile.LOWER_BOUNDS);
    fields.add(DataFile.UPPER_BOUNDS);
    fields.add(DataFile.KEY_METADATA);
    fields.add(DataFile.SPLIT_OFFSETS);
    fields.add(DataFile.EQUALITY_IDS);
    fields.add(DataFile.SORT_ORDER_ID);
    fields.add(DataFile.FIRST_ROW_ID);
    fields.add(DataFile.REFERENCED_DATA_FILE);
    fields.add(DataFile.CONTENT_OFFSET);
    fields.add(DataFile.CONTENT_SIZE);
    return Types.StructType.of(fields.build());
  }

  static class ManifestEntryWrapper<F extends ContentFile<F>>
      implements ManifestEntry<F>, StructLike {
    private final int size;
    private final Long commitSnapshotId;
    private final DataFileWrapper<?> fileWrapper;
    private ManifestEntry<F> wrapped = null;

    ManifestEntryWrapper(Long commitSnapshotId, Types.StructType partitionType) {
      this.size = entrySchema(partitionType).columns().size();
      this.commitSnapshotId = commitSnapshotId;
      this.fileWrapper = new DataFileWrapper<>(partitionType);
    }

    public ManifestEntryWrapper<F> wrap(ManifestEntry<F> entry) {
      this.wrapped = entry;
      return this;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify ManifestEntryWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      switch (pos) {
        case 0:
          return wrapped.status().id();
        case 1:
          return wrapped.snapshotId();
        case 2:
          if (wrapped.dataSequenceNumber() == null) {
            // if the entry's data sequence number is null,
            // then it will inherit the sequence number of the current commit.
            // to validate that this is correct, check that the snapshot id is either null (will
            // also be inherited) or that it matches the id of the current commit.
            Preconditions.checkState(
                wrapped.snapshotId() == null || wrapped.snapshotId().equals(commitSnapshotId),
                "Found unassigned sequence number for an entry from snapshot: %s",
                wrapped.snapshotId());

            // inheritance should work only for ADDED entries
            Preconditions.checkState(
                wrapped.status() == Status.ADDED,
                "Only entries with status ADDED can have null sequence number");

            return null;
          }
          return wrapped.dataSequenceNumber();
        case 3:
          return wrapped.fileSequenceNumber();
        case 4:
          return fileWrapper.wrap(wrapped.file());
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public Status status() {
      return wrapped.status();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public void setSnapshotId(long snapshotId) {
      wrapped.setSnapshotId(snapshotId);
    }

    @Override
    public Long dataSequenceNumber() {
      return wrapped.dataSequenceNumber();
    }

    @Override
    public void setDataSequenceNumber(long dataSequenceNumber) {
      wrapped.setDataSequenceNumber(dataSequenceNumber);
    }

    @Override
    public Long fileSequenceNumber() {
      return wrapped.fileSequenceNumber();
    }

    @Override
    public void setFileSequenceNumber(long fileSequenceNumber) {
      wrapped.setFileSequenceNumber(fileSequenceNumber);
    }

    @Override
    public F file() {
      return wrapped.file();
    }

    @Override
    public ManifestEntry<F> copy() {
      return wrapped.copy();
    }

    @Override
    public ManifestEntry<F> copyWithoutStats() {
      return wrapped.copyWithoutStats();
    }
  }

  /** Wrapper used to write DataFile or DeleteFile to v4 metadata. */
  static class DataFileWrapper<F extends ContentFile<F>> extends Delegates.DelegatingContentFile<F>
      implements ContentFile<F>, StructLike {
    private static final int PARTITION_POSITION = 3;

    private final int size;
    private final boolean hasPartition;

    DataFileWrapper(Types.StructType partitionType) {
      super(null);
      this.hasPartition = !partitionType.fields().isEmpty();
      this.size = fileType(partitionType).fields().size();
    }

    @SuppressWarnings("unchecked")
    DataFileWrapper<F> wrap(ContentFile<?> file) {
      setWrapped((F) file);
      return this;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify DataFileWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      // when the partition field is omitted, positions at or after where it would appear
      // shift down by 1, so adjust back to the canonical field ordering
      int adjusted = hasPartition ? pos : (pos >= PARTITION_POSITION ? pos + 1 : pos);
      switch (adjusted) {
        case 0:
          return wrapped.content().id();
        case 1:
          return wrapped.location();
        case 2:
          return wrapped.format() != null ? wrapped.format().lowerCaseName() : null;
        case 3:
          return wrapped.partition();
        case 4:
          return wrapped.recordCount();
        case 5:
          return wrapped.fileSizeInBytes();
        case 6:
          return wrapped.columnSizes();
        case 7:
          return wrapped.valueCounts();
        case 8:
          return wrapped.nullValueCounts();
        case 9:
          return wrapped.nanValueCounts();
        case 10:
          return wrapped.lowerBounds();
        case 11:
          return wrapped.upperBounds();
        case 12:
          return wrapped.keyMetadata();
        case 13:
          return wrapped.splitOffsets();
        case 14:
          return wrapped.equalityFieldIds();
        case 15:
          return wrapped.sortOrderId();
        case 16:
          if (wrapped.content() == FileContent.DATA) {
            return wrapped.firstRowId();
          } else {
            return null;
          }
        case 17:
          if (wrapped.content() == FileContent.POSITION_DELETES) {
            return ((DeleteFile) wrapped).referencedDataFile();
          } else {
            return null;
          }
        case 18:
          if (wrapped.content() == FileContent.POSITION_DELETES) {
            return ((DeleteFile) wrapped).contentOffset();
          } else {
            return null;
          }
        case 19:
          if (wrapped.content() == FileContent.POSITION_DELETES) {
            return ((DeleteFile) wrapped).contentSizeInBytes();
          } else {
            return null;
          }
      }
      throw new IllegalArgumentException("Unknown field ordinal: " + pos);
    }

    @Override
    public String manifestLocation() {
      return null;
    }

    @Override
    public Long pos() {
      return null;
    }
  }

  /** Wrapper used to write a data file entry to a v4 leaf manifest as a tracked file. */
  static class TrackedFileWrapper implements ManifestEntry<DataFile>, StructLike {
    private final int size;
    private final Long commitSnapshotId;
    private final boolean isPartitioned;
    private final Types.StructType statsType;
    private ManifestEntry<DataFile> wrapped = null;

    TrackedFileWrapper(Long commitSnapshotId, PartitionSpec spec) {
      this.size = manifestSchema(spec).columns().size();
      this.commitSnapshotId = commitSnapshotId;
      this.isPartitioned = spec.isPartitioned();
      this.statsType = statsType(spec.schema());
    }

    TrackedFileWrapper wrap(ManifestEntry<DataFile> entry) {
      this.wrapped = entry;
      return this;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Cannot modify TrackedFileWrapper wrapper via set");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      DataFile file = wrapped.file();
      return switch (pos) {
        case 0 -> tracking();
        case 1 -> file.content().id();
        case 2 -> file.location();
        case 3 -> file.format().lowerCaseName();
        case 4 -> file.recordCount();
        case 5 -> file.fileSizeInBytes();
        case 6 -> isPartitioned ? file.specId() : null;
        case 7 -> isPartitioned ? file.partition() : null;
        case 8 -> contentStats(file);
        case 9 -> tightStats();
        case 10 -> file.sortOrderId();
        case 11 -> file.deletionVector();
        case 12 -> null;
        case 13 -> file.keyMetadata();
        case 14 -> file.splitOffsets();
        default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      };
    }

    private Boolean tightStats() {
      if (wrapped.status() == Status.ADDED) {
        // stats for newly added files are tight
        return true;
      }

      return null;
    }

    private Tracking tracking() {
      if (wrapped.dataSequenceNumber() == null) {
        // if the entry's data sequence number is null,
        // then it will inherit the sequence number of the current commit.
        // to validate that this is correct, check that the snapshot id is either null (will
        // also be inherited) or that it matches the id of the current commit.
        Preconditions.checkState(
            wrapped.snapshotId() == null || wrapped.snapshotId().equals(commitSnapshotId),
            "Found unassigned sequence number for an entry from snapshot: %s",
            wrapped.snapshotId());

        // inheritance should work only for ADDED entries
        Preconditions.checkState(
            wrapped.status() == Status.ADDED,
            "Only entries with status ADDED can have null sequence number");
      }

      return new TrackingStruct(
          EntryStatus.fromId(wrapped.status().id()),
          wrapped.snapshotId(),
          wrapped.dataSequenceNumber(),
          wrapped.fileSequenceNumber(),
          null /* modified snapshot ID */,
          wrapped.file().firstRowId(),
          null /* deleted positions */,
          null /* replaced positions */);
    }

    private ContentStats contentStats(DataFile file) {
      ContentStatsStruct stats = new ContentStatsStruct(statsType);
      for (Types.NestedField field : statsType.fields()) {
        FieldStatsStruct<?> fieldStats = fieldStats(file, field.type().asStructType());
        if (fieldStats != null) {
          stats.setStats(fieldStats.fieldId(), fieldStats);
        }
      }

      return stats;
    }

    private static FieldStatsStruct<?> fieldStats(DataFile file, Types.StructType type) {
      FieldStatsStruct<?> stats = new FieldStatsStruct<>(type);
      int fieldId = stats.fieldId();
      boolean hasStats = false;
      List<Types.NestedField> fields = type.fields();
      for (int pos = 0; pos < fields.size(); pos += 1) {
        Types.NestedField field = fields.get(pos);
        Object value =
            switch (StatsUtil.statOffset(field.fieldId())) {
              case StatsUtil.LOWER_BOUND_OFFSET -> bound(field.type(), file.lowerBounds(), fieldId);
              case StatsUtil.UPPER_BOUND_OFFSET -> bound(field.type(), file.upperBounds(), fieldId);
              case StatsUtil.VALUE_COUNT_OFFSET -> valueOrNull(file.valueCounts(), fieldId);
              case StatsUtil.NULL_VALUE_COUNT_OFFSET ->
                  valueOrNull(file.nullValueCounts(), fieldId);
              case StatsUtil.NAN_VALUE_COUNT_OFFSET -> valueOrNull(file.nanValueCounts(), fieldId);
              case StatsUtil.TOTAL_BYTES_OFFSET -> valueOrNull(file.totalBytes(), fieldId);
              default -> null;
            };

        if (value != null) {
          stats.set(pos, value);
          hasStats = true;
        }
      }

      return hasStats ? stats : null;
    }

    // bounds for geo and variant types are not converted
    private static Object bound(Type type, Map<Integer, ByteBuffer> bounds, int fieldId) {
      ByteBuffer bound = valueOrNull(bounds, fieldId);
      if (bound == null || !type.isPrimitiveType()) {
        return null;
      }

      return Conversions.fromByteBuffer(type, bound);
    }

    private static <T> T valueOrNull(Map<Integer, T> map, int fieldId) {
      return map != null ? map.get(fieldId) : null;
    }

    @Override
    public Status status() {
      return wrapped.status();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public void setSnapshotId(long snapshotId) {
      wrapped.setSnapshotId(snapshotId);
    }

    @Override
    public Long dataSequenceNumber() {
      return wrapped.dataSequenceNumber();
    }

    @Override
    public void setDataSequenceNumber(long dataSequenceNumber) {
      wrapped.setDataSequenceNumber(dataSequenceNumber);
    }

    @Override
    public Long fileSequenceNumber() {
      return wrapped.fileSequenceNumber();
    }

    @Override
    public void setFileSequenceNumber(long fileSequenceNumber) {
      wrapped.setFileSequenceNumber(fileSequenceNumber);
    }

    @Override
    public DataFile file() {
      return wrapped.file();
    }

    @Override
    public ManifestEntry<DataFile> copy() {
      return wrapped.copy();
    }

    @Override
    public ManifestEntry<DataFile> copyWithoutStats() {
      return wrapped.copyWithoutStats();
    }
  }
}
