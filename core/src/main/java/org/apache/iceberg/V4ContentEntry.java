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

package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class V4ContentEntry implements StructLike {

  static Types.NestedField REFERENCED_FILE = optional(143, "referenced_file", Types.StringType.get());
  static Types.NestedField SPEC_ID = optional(147, "partition_spec_id", Types.IntegerType.get());
  static Types.NestedField LOCATION = optional(100, "location", Types.StringType.get());
  static Types.NestedField FILE_FORMAT = optional(101, "file_format", Types.StringType.get());
  static Types.NestedField RECORD_COUNT = required(103, "record_count", Types.LongType.get());
  static Types.NestedField FILE_SIZE = optional(104, "file_size", Types.LongType.get());
  // content_stats
  static Types.NestedField KEY_METADATA = optional(131, "key_metadata", Types.BinaryType.get());
  static Types.NestedField OFFSETS = optional(132, "offsets", Types.ListType.ofOptional(133, Types.LongType.get()));
  static Types.NestedField EQUALITY_IDS = optional(135, "equality_ids", Types.ListType.ofOptional(136, Types.IntegerType.get()));
  static Types.NestedField SORT_ORDER_ID = optional(140, "sort_order_id", Types.IntegerType.get());
  static Types.NestedField FIRST_ROW_ID = optional(142, "first_row_id", Types.LongType.get());
  static Types.NestedField CONTENT_OFFSET = optional(144, "content_offset", Types.LongType.get());
  static Types.NestedField CONTENT_SIZE_IN_BYTES = optional(145, "content_size_in_bytes", Types.LongType.get());
  // manifest stats
  static Types.NestedField ADDED_FILES_COUNT = optional(504, "added_files_count", Types.IntegerType.get());
  static Types.NestedField EXISTING_FILES_COUNT = optional(505, "existing_files_count", Types.IntegerType.get());
  static Types.NestedField DELETED_FILES_COUNT = optional(506, "deleted_files_count", Types.IntegerType.get());
  static Types.NestedField ADDED_ROWS_COUNT = optional(512, "added_rows_count", Types.LongType.get());
  static Types.NestedField EXISTING_ROWS_COUNT = optional(513, "existing_rows_count", Types.LongType.get());
  static Types.NestedField DELETED_ROWS_COUNT = optional(514, "deleted_rows_count", Types.LongType.get());


  public static Types.NestedField fieldFor(Schema dataSchema) {
    Types.NestedField contentStats = TypeUtil.visit(dataSchema, new ContentStatsSchemaVisitor());
    Types.NestedField manifestStats = optional(515, "manifest_stats", Types.StructType.of(
            ADDED_FILES_COUNT,
            EXISTING_FILES_COUNT,
            DELETED_FILES_COUNT,
            ADDED_ROWS_COUNT,
            EXISTING_ROWS_COUNT,
            DELETED_ROWS_COUNT
    ));

    return required(2, "content_entry", Types.StructType.of(
            LOCATION,
            FILE_FORMAT,
            SPEC_ID,
            RECORD_COUNT,
            FILE_SIZE,
            contentStats,
            manifestStats,
            KEY_METADATA,
            OFFSETS,
            EQUALITY_IDS,
            SORT_ORDER_ID,
            FIRST_ROW_ID,
            REFERENCED_FILE,
            CONTENT_OFFSET,
            CONTENT_SIZE_IN_BYTES
    ));
  }

  static class ContentStatsSchemaVisitor extends TypeUtil.SchemaVisitor<Types.NestedField> {

    @Override
    public Types.NestedField schema(Schema schema, Types.NestedField structResult) {
      return structResult;
    }

    @Override
    public Types.NestedField struct(Types.StructType struct, List<Types.NestedField> fieldResults) {
      return optional(608, "content_stats", Types.StructType.of(fieldResults));
    }

    @Override
    public Types.NestedField field(Types.NestedField field, Types.NestedField fieldResult) {

      int baseId = 10_000 + 200 * field.fieldId();

      Types.StructType contentStruct =
              Types.StructType.of(
                      optional(baseId++, "size", Types.LongType.get()),
                      optional(baseId++, "record_count", Types.IntegerType.get()),
                      optional(baseId++, "value_count", Types.IntegerType.get()),
                      optional(baseId++, "null_value_count", Types.IntegerType.get()),
                      optional(baseId++, "lower_bound", field.type()),
                      optional(baseId++, "upper_bound", field.type()));

      return Types.NestedField.optional(baseId + 600, field.fieldId() + "_f", contentStruct);
    }
  }

  private int partitionSpecId;
  private String location;
  private FileFormat fileFormat;
  private Long recordCount;
  private Long fileSize;
  private ContentStats contentStats;
  private ManifestStats manifestStats;
  private byte[] keyMetadata;
  private List<Long> offsets;
  private List<Integer> equalityIds;
  private int sortOrderId;
  private long firstRowId;
  private String referencedFile;
  private Long contentOffset;
  private Long contentSize;

  @Override
  public int size() {
    return 15;
  }

  private Object get(int pos) {
    switch (pos) {
      case 0: return location;
      case 1: return fileFormat.name();
      case 2: return partitionSpecId;
      case 3: return recordCount;
      case 4: return fileSize;
      case 5: return contentStats;
      case 6: return manifestStats;
      case 7: return keyMetadata;
      case 8: return offsets;
      case 9: return equalityIds;
      case 10: return sortOrderId;
      case 11: return firstRowId;
      case 12: return referencedFile;
      case 13: return contentOffset;
      case 14: return contentSize;
      default: return null;
    }
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public <T> void set(int pos, T value) {}

  public static class ManifestStats implements StructLike {
    private final Integer addedFilesCount;
    private final Integer existingFilesCount;
    private final Integer deletedFilesCount;
    private final Long addRowsCount;
    private final Long existingRowsCount;
    private final Long deletedRowsCount;

    public ManifestStats(Integer addedFilesCount, Integer existingFilesCount, Integer deletedFilesCount, Long addRowsCount, Long existingRowsCount, Long deletedRowsCount) {
      this.addedFilesCount = addedFilesCount;
      this.existingFilesCount = existingFilesCount;
      this.deletedFilesCount = deletedFilesCount;
      this.addRowsCount = addRowsCount;
      this.existingRowsCount = existingRowsCount;
      this.deletedRowsCount = deletedRowsCount;
    }

    @Override
    public int size() {
      return 0;
    }

    private Object get(int pos) {
      switch (pos) {
        case 0: return addedFilesCount;
        case 1: return existingFilesCount;
        case 2: return deletedFilesCount;
        case 3: return addRowsCount;
        case 4: return existingRowsCount;
        case 5: return deletedRowsCount;
        default: return null;
      }
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    @Override
    public <T> void set(int pos, T value) {

    }
  }

  public static class ContentStats implements StructLike {
    private final FieldStats[] fieldStats;

    public ContentStats(int size) {
      this.fieldStats = new FieldStats[size + 1];
    }

    @Override
    public int size() {
      return fieldStats.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return (T) fieldStats[pos];
    }

    @Override
    public <T> void set(int pos, T value) {
      fieldStats[pos] = (FieldStats) value;
    }

    public List<FieldStats> fieldStats() {
      return Arrays.asList(fieldStats);
    }
  }

  public static class FieldStats implements StructLike {
    private Long size;
    private Integer recordCount;
    private Integer valueCount;
    private Integer nullValueCount;
    private Object lowerBound;
    private Object upperBound;

    @Override
    public int size() {
      return 6;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      switch (pos) {
        case 0:
          return size;
        case 1:
          return recordCount;
        case 2:
          return valueCount;
        case 3:
          return nullValueCount;
        case 4:
          return lowerBound;
        case 5:
          return upperBound;
        default:
          return null;
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException();
    }

    public FieldStats size(Long size) {
      this.size = size;
      return this;
    }

    public Integer recordCount() {
      return recordCount;
    }

    public FieldStats recordCount(Integer recordCount) {
      this.recordCount = recordCount;
      return this;
    }

    public Integer valueCount() {
      return valueCount;
    }

    public FieldStats valueCount(Integer valueCount) {
      this.valueCount = valueCount;
      return this;
    }

    public Integer nullValueCount() {
      return nullValueCount;
    }

    public FieldStats nullValueCount(Integer nullValueCount) {
      this.nullValueCount = nullValueCount;
      return this;
    }

    public Object lowerBound() {
      return lowerBound;
    }

    public FieldStats lowerBound(Object lowerBound) {
      this.lowerBound = lowerBound;
      return this;
    }

    public Object upperBound() {
      return upperBound;
    }

    public FieldStats upperBound(Object upperBound) {
      this.upperBound = upperBound;
      return this;
    }
  }

  public int partitionSpecId() {
    return partitionSpecId;
  }

  public V4ContentEntry partitionSpecId(int partitionSpecId) {
    this.partitionSpecId = partitionSpecId;
    return this;
  }

  public String location() {
    return location;
  }

  public V4ContentEntry location(String location) {
    this.location = location;
    return this;
  }

  public FileFormat fileFormat() {
    return fileFormat;
  }

  public V4ContentEntry fileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
    return this;
  }

  public Long recordCount() {
    return recordCount;
  }

  public V4ContentEntry recordCount(Long recordCount) {
    this.recordCount = recordCount;
    return this;
  }

  public Long fileSize() {
    return fileSize;
  }

  public V4ContentEntry fileSize(Long fileSize) {
    this.fileSize = fileSize;
    return this;
  }

  public ContentStats contentStats() {
    return contentStats;
  }

  public V4ContentEntry contentStats(ContentStats contentStats) {
    this.contentStats = contentStats;
    return this;
  }

  public ManifestStats manifestStats() {
    return manifestStats;
  }

  public V4ContentEntry manifestStats(ManifestStats manifestStats) {
    this.manifestStats = manifestStats;
    return this;
  }

  public byte[] keyMetadata() {
    return keyMetadata;
  }

  public V4ContentEntry keyMetadata(byte[] keyMetadata) {
    this.keyMetadata = keyMetadata;
    return this;
  }

  public List<Long> offsets() {
    return offsets;
  }

  public V4ContentEntry offsets(List<Long> offsets) {
    this.offsets = offsets;
    return this;
  }

  public List<Integer> equalityIds() {
    return equalityIds;
  }

  public V4ContentEntry equalityIds(List<Integer> equalityIds) {
    this.equalityIds = equalityIds;
    return this;
  }

  public int sortOrderId() {
    return sortOrderId;
  }

  public V4ContentEntry sortOrderId(int sortOrderId) {
    this.sortOrderId = sortOrderId;
    return this;
  }

  public long firstRowId() {
    return firstRowId;
  }

  public V4ContentEntry firstRowId(long firstRowId) {
    this.firstRowId = firstRowId;
    return this;
  }

  public String referencedFile() {
    return referencedFile;
  }

  public V4ContentEntry referencedFile(String referencedFile) {
    this.referencedFile = referencedFile;
    return this;
  }

  public Long contentOffset() {
    return contentOffset;
  }

  public V4ContentEntry contentOffset(Long contentOffset) {
    this.contentOffset = contentOffset;
    return this;
  }

  public Long contentSize() {
    return contentSize;
  }

  public V4ContentEntry contentSize(Long contentSize) {
    this.contentSize = contentSize;
    return this;
  }
}
