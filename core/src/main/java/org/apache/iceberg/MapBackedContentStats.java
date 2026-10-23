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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Reusable {@link ContentStats} view over a {@link ContentFile}'s stat maps and partition values.
 */
class MapBackedContentStats implements ContentStats {
  private final Schema tableSchema;
  private final Map<Integer, FieldStats<?>> statsById = Maps.newHashMap();
  private final Map<Integer, PartitionStats> partitionStatsById = Maps.newHashMap();

  private Types.StructType type;
  private Map<Integer, Long> valueCounts;
  private Map<Integer, Long> nullValueCounts;
  private Map<Integer, Long> nanValueCounts;
  private Map<Integer, Integer> avgValueSizes;
  private Map<Integer, ByteBuffer> lowerBounds;
  private Map<Integer, ByteBuffer> upperBounds;
  private PartitionStatsPlan plan = PartitionStatsPlan.EMPTY;
  private StructLike partition;
  private long recordCount;

  MapBackedContentStats(Schema tableSchema) {
    Preconditions.checkArgument(tableSchema != null, "Invalid table schema: null");
    this.tableSchema = tableSchema;
  }

  MapBackedContentStats wrap(ContentFile<?> file) {
    return wrap(file, PartitionStatsPlan.EMPTY);
  }

  MapBackedContentStats wrap(ContentFile<?> file, PartitionStatsPlan newPlan) {
    this.valueCounts = file.valueCounts();
    this.nullValueCounts = file.nullValueCounts();
    this.nanValueCounts = file.nanValueCounts();
    this.avgValueSizes = file.avgValueSizes();
    this.lowerBounds = file.lowerBounds();
    this.upperBounds = file.upperBounds();
    // partition values are read when stats are first read, so the file's partition tuple must not
    // change until then
    this.partition = file.partition();
    this.recordCount = file.recordCount();
    this.plan = newPlan;
    this.type = null;
    partitionStatsById.clear();
    return this;
  }

  @Override
  public Iterable<FieldStats<?>> fieldStats() {
    return Iterables.filter(Iterables.transform(statsFieldIds(), this::statsFor), Objects::nonNull);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> FieldStats<T> statsFor(int fieldId) {
    // Schema is fixed for this instance; wrap() rebinds the metric maps. An id absent
    // from the current file is left uncached so a later file can still surface it.
    if (!hasStats(fieldId)) {
      return null;
    }

    if (!statsById.containsKey(fieldId)) {
      statsById.put(fieldId, createFieldStats(fieldId));
    }

    return (FieldStats<T>) statsById.get(fieldId);
  }

  private FieldStats<?> createFieldStats(int fieldId) {
    Types.NestedField field = tableSchema.findField(fieldId);
    // A file can carry metrics for an id this schema does not have. Skip it.
    if (field == null) {
      return null;
    }

    Type fieldType = field.type();
    Types.StructType struct =
        StatsUtil.fieldStatsStruct(fieldType, StatsUtil.toBaseId(fieldId), MetricsModes.Full.get());
    // null means a struct, list, or map, or an id outside the stats window. The schema is bound
    // once during wrapper creation, so the cached null stays valid across wrap().
    if (struct == null) {
      return null;
    }

    return new MapBackedFieldStats<>(fieldId, fieldType, struct);
  }

  @Override
  public Types.StructType type() {
    if (type == null) {
      this.type = StatsUtil.statsReadSchema(tableSchema, statsFieldIds());
    }

    return type;
  }

  @Override
  public ContentStats copy() {
    throw new UnsupportedOperationException("copy is not implemented");
  }

  @Override
  public ContentStats copy(Set<Integer> fieldIds) {
    throw new UnsupportedOperationException("copy is not implemented");
  }

  private boolean hasStats(int fieldId) {
    return plan.columnsById.containsKey(fieldId)
        || containsId(valueCounts, fieldId)
        || containsId(nullValueCounts, fieldId)
        || containsId(nanValueCounts, fieldId)
        || containsId(avgValueSizes, fieldId)
        || containsId(lowerBounds, fieldId)
        || containsId(upperBounds, fieldId);
  }

  private Set<Integer> statsFieldIds() {
    Set<Integer> ids =
        Sets.newHashSetWithExpectedSize(valueCounts == null ? 0 : valueCounts.size());
    ids.addAll(plan.columnsById.keySet());
    addKeys(ids, valueCounts);
    addKeys(ids, nullValueCounts);
    addKeys(ids, nanValueCounts);
    addKeys(ids, avgValueSizes);
    addKeys(ids, lowerBounds);
    addKeys(ids, upperBounds);
    return ids;
  }

  private PartitionStats partitionStats(int fieldId) {
    SourceColumn column = plan.columnsById.get(fieldId);
    if (column == null) {
      return null;
    }

    PartitionStats stats = partitionStatsById.get(fieldId);
    if (stats == null) {
      stats = derivePartitionStats(fieldId, column);
      partitionStatsById.put(fieldId, stats);
    }

    return stats;
  }

  private PartitionStats derivePartitionStats(int fieldId, SourceColumn column) {
    Object recordedLower = decode(column.type(), lowerBounds, fieldId);
    Object recordedUpper = decode(column.type(), upperBounds, fieldId);
    Comparator<Object> comparator = column.comparator();
    boolean hasRange = false;
    Object lower = null;
    Object upper = null;
    for (int i = 0; i < column.positions().size(); i += 1) {
      Object value = partition.get(column.positions().get(i), Object.class);
      if (value == null) {
        // a null partition value means every source value in the file is null
        return new PartitionStats(recordCount, recordCount, null, null);
      }

      // keep the range that fits every partition field on this column, where a null upper has no
      // limit and a NaN identity value has no range
      StatsUtil.PartitionSourceBounds bounds =
          StatsUtil.sourceBoundsFor(tableSchema, column.fields().get(i), value);
      if (bounds != null) {
        hasRange = true;
        lower = max(comparator, lower, bounds.lower());
        upper = min(comparator, upper, bounds.upper());
      }
    }

    if (!hasRange) {
      return new PartitionStats(
          count(valueCounts, fieldId),
          count(nullValueCounts, fieldId),
          recordedLower,
          recordedUpper);
    }

    lower = max(comparator, lower, recordedLower);
    upper = min(comparator, upper, recordedUpper);
    if (lower != null && upper != null && comparator.compare(lower, upper) > 0) {
      // the ranges can only miss each other when a partition-derived range is wrong, such as after
      // integer overflow in truncate, so keep the file's recorded column bounds
      lower = recordedLower;
      upper = recordedUpper;
    }

    // a non-null partition value means every source value in the file is non-null
    return new PartitionStats(recordCount, 0L, lower, upper);
  }

  private static Object decode(Type type, Map<Integer, ByteBuffer> bounds, int fieldId) {
    return bounds == null ? null : Conversions.fromByteBuffer(type, bounds.get(fieldId));
  }

  private static Long count(Map<Integer, Long> counts, int fieldId) {
    return counts == null ? null : counts.get(fieldId);
  }

  private static Object max(Comparator<Object> comparator, Object left, Object right) {
    if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    }

    return comparator.compare(left, right) >= 0 ? left : right;
  }

  private static Object min(Comparator<Object> comparator, Object left, Object right) {
    if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    }

    return comparator.compare(left, right) <= 0 ? left : right;
  }

  private static void addKeys(Set<Integer> ids, Map<Integer, ?> map) {
    if (map != null) {
      ids.addAll(map.keySet());
    }
  }

  private static boolean containsId(Map<Integer, ?> map, int id) {
    return map != null && map.containsKey(id);
  }

  /** Reusable {@link FieldStats} view over one field's entries in a {@link ContentFile}'s maps. */
  private class MapBackedFieldStats<T> implements FieldStats<T> {
    private final int fieldId;
    private final Types.StructType struct;
    private final Type fieldType;
    private final Type boundType;

    private MapBackedFieldStats(int fieldId, Type fieldType, Types.StructType struct) {
      this.fieldId = fieldId;
      this.fieldType = fieldType;
      this.struct = struct;
      this.boundType = struct.fieldType(StatsUtil.LOWER_BOUND_NAME);
    }

    @Override
    public int fieldId() {
      return fieldId;
    }

    @Override
    public Types.StructType type() {
      return struct;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T lowerBound() {
      if (boundType == null) {
        return null;
      }

      PartitionStats stats = partitionStats(fieldId);
      return (T) (stats != null ? stats.lowerBound() : decode(fieldType, lowerBounds, fieldId));
    }

    @Override
    @SuppressWarnings("unchecked")
    public T upperBound() {
      if (boundType == null) {
        return null;
      }

      PartitionStats stats = partitionStats(fieldId);
      return (T) (stats != null ? stats.upperBound() : decode(fieldType, upperBounds, fieldId));
    }

    @Override
    public boolean tightBounds() {
      return false;
    }

    @Override
    public boolean hasValueCount() {
      return valueCountOrNull() != null;
    }

    @Override
    public long valueCount() {
      return valueCountOrNull();
    }

    @Override
    public boolean hasNullValueCount() {
      return nullValueCountOrNull() != null;
    }

    @Override
    public long nullValueCount() {
      return nullValueCountOrNull();
    }

    @Override
    public boolean hasNanValueCount() {
      return count(nanValueCounts, fieldId) != null;
    }

    @Override
    public long nanValueCount() {
      return count(nanValueCounts, fieldId);
    }

    @Override
    public Integer avgValueSizeInBytes() {
      return avgValueSizes == null ? null : avgValueSizes.get(fieldId);
    }

    @Override
    public FieldStats<T> copy() {
      throw new UnsupportedOperationException("copy is not implemented");
    }

    private Long valueCountOrNull() {
      PartitionStats stats = partitionStats(fieldId);
      return stats != null ? stats.valueCount() : count(valueCounts, fieldId);
    }

    private Long nullValueCountOrNull() {
      PartitionStats stats = partitionStats(fieldId);
      return stats != null ? stats.nullValueCount() : count(nullValueCounts, fieldId);
    }
  }

  private record PartitionStats(
      Long valueCount, Long nullValueCount, Object lowerBound, Object upperBound) {}

  private record SourceColumn(
      Type type,
      Comparator<Object> comparator,
      List<Integer> positions,
      List<PartitionField> fields) {}

  /** The partition fields of a spec whose values can fill in stats for their source columns. */
  static class PartitionStatsPlan {
    static final PartitionStatsPlan EMPTY = new PartitionStatsPlan(ImmutableMap.of());

    private final Map<Integer, SourceColumn> columnsById;

    private PartitionStatsPlan(Map<Integer, SourceColumn> columnsById) {
      this.columnsById = columnsById;
    }

    static PartitionStatsPlan of(Schema tableSchema, PartitionSpec spec) {
      Map<Integer, List<Integer>> positionsById = Maps.newLinkedHashMap();
      List<PartitionField> fields = spec.fields();
      for (int pos = 0; pos < fields.size(); pos += 1) {
        // skip transforms that do not preserve order, such as bucket and void, and source columns
        // that are not in the table schema
        PartitionField field = fields.get(pos);
        int sourceId = field.sourceId();
        if (field.transform().preservesOrder() && tableSchema.findField(sourceId) != null) {
          positionsById.computeIfAbsent(sourceId, id -> Lists.newArrayList()).add(pos);
        }
      }

      ImmutableMap.Builder<Integer, SourceColumn> columns = ImmutableMap.builder();
      positionsById.forEach(
          (sourceId, positions) -> {
            Type type = tableSchema.findType(sourceId);
            List<PartitionField> sourceFields = positions.stream().map(fields::get).toList();
            columns.put(
                sourceId,
                new SourceColumn(
                    type,
                    Comparators.forType(type.asPrimitiveType()),
                    List.copyOf(positions),
                    sourceFields));
          });

      return new PartitionStatsPlan(columns.build());
    }

    boolean isEmpty() {
      return columnsById.isEmpty();
    }
  }
}
