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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

class TestMapBackedContentStatsFromPartition {
  private static final int ID = 1;
  private static final int TS = 2;
  private static final int CATEGORY = 3;
  private static final long RECORD_COUNT = 100L;

  private static final Schema SCHEMA =
      new Schema(
          optional(ID, "id", Types.IntegerType.get()),
          optional(TS, "ts", Types.TimestampType.withZone()),
          optional(CATEGORY, "category", Types.StringType.get()));

  private static final int DAY = DateTimeUtil.isoDateToDays("2026-10-15");
  private static final long DAY_START =
      DateTimeUtil.isoTimestamptzToMicros("2026-10-15T00:00:00+00:00");
  private static final long DAY_END =
      DateTimeUtil.isoTimestamptzToMicros("2026-10-16T00:00:00+00:00") - 1;
  private static final long MIDDAY =
      DateTimeUtil.isoTimestamptzToMicros("2026-10-15T12:00:00+00:00");

  @Test
  void partitionValuesGiveBoundsAndCounts() {
    PartitionSpec spec = spec(b -> b.identity("category").day("ts"));
    MapBackedContentStats stats = wrap(spec, file(spec, noMetrics(), "books", DAY));

    FieldStats<?> category = stats.statsFor(CATEGORY);
    assertThat(category.lowerBound()).isEqualTo("books");
    assertThat(category.upperBound()).isEqualTo("books");
    assertThat(category.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(category.nullValueCount()).isEqualTo(0L);

    FieldStats<?> ts = stats.statsFor(TS);
    assertThat(ts.lowerBound()).isEqualTo(DAY_START);
    assertThat(ts.upperBound()).isEqualTo(DAY_END);
    assertThat(ts.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(ts.nullValueCount()).isEqualTo(0L);
  }

  @Test
  void typeIncludesPartitionSourceColumns() {
    PartitionSpec spec = spec(b -> b.identity("category").day("ts"));
    MapBackedContentStats stats = wrap(spec, file(spec, noMetrics(), "books", DAY));

    assertThat(stats.hasPartitionStats()).isTrue();
    assertThat(stats.fieldStats())
        .extracting(FieldStats::fieldId)
        .containsExactlyInAnyOrder(TS, CATEGORY);
    assertThat(stats.type().fields())
        .containsExactlyInAnyOrderElementsOf(
            StatsUtil.statsReadSchema(SCHEMA, List.of(TS, CATEGORY)).fields());
  }

  @Test
  void fileBoundsInsidePartitionRangeAreKept() {
    PartitionSpec spec = spec(b -> b.day("ts"));
    Metrics metrics =
        metrics(
            ImmutableMap.of(TS, bytes(Types.TimestampType.withZone(), MIDDAY)),
            ImmutableMap.of(TS, bytes(Types.TimestampType.withZone(), MIDDAY + 1_000L)));
    MapBackedContentStats stats = wrap(spec, file(spec, metrics, DAY));

    FieldStats<?> ts = stats.statsFor(TS);
    assertThat(ts.lowerBound()).isEqualTo(MIDDAY);
    assertThat(ts.upperBound()).isEqualTo(MIDDAY + 1_000L);
  }

  @Test
  void looseFileBoundsAreTightenedToPartitionRange() {
    PartitionSpec spec = spec(b -> b.identity("category"));
    Metrics metrics =
        metrics(
            ImmutableMap.of(CATEGORY, bytes(Types.StringType.get(), "b")),
            ImmutableMap.of(CATEGORY, bytes(Types.StringType.get(), "c")));
    MapBackedContentStats stats = wrap(spec, file(spec, metrics, "books"));

    FieldStats<?> category = stats.statsFor(CATEGORY);
    assertThat(category.lowerBound()).isEqualTo("books");
    assertThat(category.upperBound()).isEqualTo("books");
  }

  @Test
  void fileBoundsOutsidePartitionRangeDeferToPartition() {
    PartitionSpec spec = spec(b -> b.day("ts"));
    long otherDay = DateTimeUtil.isoTimestamptzToMicros("2026-10-20T12:00:00+00:00");
    Metrics metrics =
        metrics(
            ImmutableMap.of(TS, bytes(Types.TimestampType.withZone(), otherDay)),
            ImmutableMap.of(TS, bytes(Types.TimestampType.withZone(), otherDay + 1_000L)));
    MapBackedContentStats stats = wrap(spec, file(spec, metrics, DAY));

    FieldStats<?> ts = stats.statsFor(TS);
    assertThat(ts.lowerBound()).isEqualTo(DAY_START);
    assertThat(ts.upperBound()).isEqualTo(DAY_END);
  }

  @Test
  void partitionCountsOverrideFileCounts() {
    PartitionSpec spec = spec(b -> b.identity("category"));
    Metrics metrics =
        new Metrics(
            RECORD_COUNT,
            null,
            ImmutableMap.of(CATEGORY, 7L),
            ImmutableMap.of(CATEGORY, 3L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of());
    MapBackedContentStats stats = wrap(spec, file(spec, metrics, "books"));

    FieldStats<?> category = stats.statsFor(CATEGORY);
    assertThat(category.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(category.nullValueCount()).isEqualTo(0L);
  }

  @Test
  void nullPartitionValueMeansAllValuesAreNull() {
    PartitionSpec spec = spec(b -> b.identity("category"));
    MapBackedContentStats stats = wrap(spec, file(spec, noMetrics(), (Object) null));

    FieldStats<?> category = stats.statsFor(CATEGORY);
    assertThat(category.lowerBound()).isNull();
    assertThat(category.upperBound()).isNull();
    assertThat(category.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(category.nullValueCount()).isEqualTo(RECORD_COUNT);
  }

  @Test
  void partitionFieldsOnTheSameSourceColumnAreIntersected() {
    PartitionSpec spec =
        spec(b -> b.truncate("category", 2, "short").truncate("category", 4, "long"));
    MapBackedContentStats stats = wrap(spec, file(spec, noMetrics(), "bo", "book"));

    FieldStats<?> category = stats.statsFor(CATEGORY);
    assertThat(category.lowerBound()).isEqualTo("book");
    assertThat(category.upperBound()).isEqualTo("bool");
  }

  @Test
  void columnsWithoutPartitionFieldsKeepFileStats() {
    PartitionSpec spec = spec(b -> b.identity("category"));
    Metrics metrics =
        new Metrics(
            RECORD_COUNT,
            null,
            ImmutableMap.of(ID, 90L),
            ImmutableMap.of(ID, 10L),
            ImmutableMap.of(),
            ImmutableMap.of(ID, bytes(Types.IntegerType.get(), 1)),
            ImmutableMap.of(ID, bytes(Types.IntegerType.get(), 9)));
    MapBackedContentStats stats = wrap(spec, file(spec, metrics, "books"));

    FieldStats<?> id = stats.statsFor(ID);
    assertThat(id.lowerBound()).isEqualTo(1);
    assertThat(id.upperBound()).isEqualTo(9);
    assertThat(id.valueCount()).isEqualTo(90L);
    assertThat(id.nullValueCount()).isEqualTo(10L);
  }

  @Test
  void bucketPartitionFieldIsRejected() {
    PartitionSpec spec = spec(b -> b.bucket("category", 16));
    DataFile file = file(spec, noMetrics(), 3);

    assertThatThrownBy(() -> wrap(spec, file))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("bucket");
  }

  @Test
  void voidPartitionFieldHasNoStats() {
    PartitionSpec spec = spec(b -> b.alwaysNull("category"));
    MapBackedContentStats stats = wrap(spec, file(spec, noMetrics(), (Object) null));

    assertThat(stats.hasPartitionStats()).isFalse();
    assertThat(stats.statsFor(CATEGORY)).isNull();
  }

  @Test
  void unknownSpecIdIsRejected() {
    PartitionSpec spec = spec(b -> b.identity("category"));
    DataFile file = file(spec, noMetrics(), "books");

    assertThatThrownBy(() -> new MapBackedContentStats(SCHEMA, ImmutableMap.of()).wrap(file))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find partition spec for spec ID: " + spec.specId());
  }

  @Test
  void sourceColumnMissingFromTableSchemaIsSkipped() {
    Schema historical =
        new Schema(
            optional(ID, "id", Types.IntegerType.get()),
            optional(4, "dropped", Types.StringType.get()));
    PartitionSpec spec =
        PartitionSpec.builderFor(historical).withSpecId(1).identity("dropped").build();
    MapBackedContentStats stats = wrap(spec, file(spec, noMetrics(), "gone"));

    assertThat(stats.hasPartitionStats()).isFalse();
    assertThat(stats.statsFor(4)).isNull();
  }

  @Test
  void partitionStatsDoNotCarryOverToTheNextFile() {
    PartitionSpec partitioned = spec(b -> b.identity("category"));
    PartitionSpec unpartitioned = PartitionSpec.unpartitioned();
    MapBackedContentStats stats =
        new MapBackedContentStats(
            SCHEMA,
            ImmutableMap.of(
                partitioned.specId(), partitioned, unpartitioned.specId(), unpartitioned));

    stats.wrap(file(partitioned, noMetrics(), "books"));
    assertThat(stats.statsFor(CATEGORY)).isNotNull();

    stats.wrap(file(unpartitioned, noMetrics()));
    assertThat(stats.hasPartitionStats()).isFalse();
    assertThat(stats.statsFor(CATEGORY)).isNull();
  }

  private static PartitionSpec spec(UnaryOperator<PartitionSpec.Builder> transforms) {
    return transforms.apply(PartitionSpec.builderFor(SCHEMA).withSpecId(1)).build();
  }

  private static MapBackedContentStats wrap(PartitionSpec spec, DataFile file) {
    return new MapBackedContentStats(SCHEMA, ImmutableMap.of(spec.specId(), spec)).wrap(file);
  }

  private static DataFile file(PartitionSpec spec, Metrics metrics, Object... partitionValues) {
    PartitionData partition = new PartitionData(spec.partitionType());
    for (int pos = 0; pos < partitionValues.length; pos += 1) {
      partition.set(pos, partitionValues[pos]);
    }

    return new GenericDataFile(
        spec.specId(),
        "s3://bucket/data/file.parquet",
        FileFormat.PARQUET,
        partition,
        1024L,
        metrics,
        null,
        ImmutableList.of(0L),
        null,
        null);
  }

  private static Metrics noMetrics() {
    return new Metrics(RECORD_COUNT);
  }

  private static Metrics metrics(
      Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds) {
    return new Metrics(
        RECORD_COUNT,
        null,
        ImmutableMap.of(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        lowerBounds,
        upperBounds);
  }

  private static ByteBuffer bytes(Type type, Object value) {
    return Conversions.toByteBuffer(type, value);
  }
}
