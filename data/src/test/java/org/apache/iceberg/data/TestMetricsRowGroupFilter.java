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
package org.apache.iceberg.data;

import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class TestMetricsRowGroupFilter {

  private static final Types.StructType STRUCT_FIELD_TYPE =
      Types.StructType.of(Types.NestedField.required(8, "int_field", IntegerType.get()));

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          optional(2, "no_stats_parquet", StringType.get()),
          required(3, "required", StringType.get()),
          optional(4, "all_nulls", DoubleType.get()),
          optional(5, "some_nulls", StringType.get()),
          optional(6, "no_nulls", StringType.get()),
          optional(7, "struct_not_null", STRUCT_FIELD_TYPE),
          optional(9, "not_in_file", FloatType.get()),
          optional(10, "str", StringType.get()),
          optional(
              11,
              "map_not_null",
              Types.MapType.ofRequired(12, 13, StringType.get(), IntegerType.get())),
          optional(14, "all_nans", DoubleType.get()),
          optional(15, "some_nans", FloatType.get()),
          optional(16, "no_nans", DoubleType.get()),
          optional(17, "some_double_nans", DoubleType.get()));

  protected static final Types.StructType UNDERSCORE_STRUCT_FIELD_TYPE =
      Types.StructType.of(Types.NestedField.required(8, "_int_field", IntegerType.get()));

  protected static final Schema FILE_SCHEMA =
      new Schema(
          required(1, "_id", IntegerType.get()),
          optional(2, "_no_stats_parquet", StringType.get()),
          required(3, "_required", StringType.get()),
          optional(4, "_all_nulls", DoubleType.get()),
          optional(5, "_some_nulls", StringType.get()),
          optional(6, "_no_nulls", StringType.get()),
          optional(7, "_struct_not_null", UNDERSCORE_STRUCT_FIELD_TYPE),
          optional(10, "_str", StringType.get()),
          optional(14, "_all_nans", Types.DoubleType.get()),
          optional(15, "_some_nans", FloatType.get()),
          optional(16, "_no_nans", Types.DoubleType.get()),
          optional(17, "_some_double_nans", Types.DoubleType.get()));

  protected static final String TOO_LONG_FOR_STATS_PARQUET;

  static {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i += 1) {
      sb.append(UUID.randomUUID());
    }
    TOO_LONG_FOR_STATS_PARQUET = sb.toString();
  }

  protected static final int INT_MIN_VALUE = 30;
  protected static final int INT_MAX_VALUE = 79;

  @TempDir protected File tempDir;

  @BeforeEach
  public void setup() throws IOException {
    GenericRecordBuilder builder = new GenericRecordBuilder(convert(FILE_SCHEMA, "table"));
    try (FileAppender<GenericData.Record> appender = createFileAppender()) {
      // create 50 records
      for (int i = 0; i < INT_MAX_VALUE - INT_MIN_VALUE + 1; i += 1) {
        builder.set("_id", INT_MIN_VALUE + i); // min=30, max=79, num-nulls=0
        builder.set(
            "_no_stats_parquet",
            TOO_LONG_FOR_STATS_PARQUET); // value longer than 4k will produce no stats
        // in Parquet
        builder.set("_required", "req"); // required, always non-null
        builder.set("_all_nulls", null); // never non-null
        builder.set("_some_nulls", (i % 10 == 0) ? null : "some"); // includes some null values
        builder.set("_no_nulls", ""); // optional, but always non-null
        builder.set("_all_nans", Double.NaN); // never non-nan
        builder.set("_some_nans", (i % 10 == 0) ? Float.NaN : 2F); // includes some nan values
        builder.set(
            "_some_double_nans", (i % 10 == 0) ? Double.NaN : 2D); // includes some nan values
        builder.set("_no_nans", 3D); // optional, but always non-nan
        builder.set("_str", i + "str" + i);

        GenericRecord structNotNull = GenericRecord.create(UNDERSCORE_STRUCT_FIELD_TYPE);
        structNotNull.setField("_int_field", INT_MIN_VALUE + i);
        builder.set("_struct_not_null", structNotNull); // struct with int

        appender.add(builder.build());
      }
    }
  }

  protected void createFile() throws IOException {
    
  }

  @Test
  public void testAllNulls() {
    boolean shouldRead;

    shouldRead = shouldRead(notNull("all_nulls"));
    assertThat(shouldRead).as("Should skip: no non-null value in all null column").isFalse();

    shouldRead = shouldRead(notNull("some_nulls"));
    assertThat(shouldRead)
        .as("Should read: column with some nulls contains a non-null value")
        .isTrue();

    shouldRead = shouldRead(notNull("no_nulls"));
    assertThat(shouldRead).as("Should read: non-null column contains a non-null value").isTrue();

    shouldRead = shouldRead(notNull("map_not_null"));
    assertThat(shouldRead).as("Should read: map type is not skipped").isTrue();

    shouldRead = shouldRead(notNull("struct_not_null"));
    assertThat(shouldRead).as("Should read: struct type is not skipped").isTrue();
  }

  @Test
  public void testNoNulls() {
    boolean shouldRead = shouldRead(isNull("all_nulls"));
    assertThat(shouldRead).as("Should read: at least one null value in all null column").isTrue();

    shouldRead = shouldRead(isNull("some_nulls"));
    assertThat(shouldRead).as("Should read: column with some nulls contains a null value").isTrue();

    shouldRead = shouldRead(isNull("no_nulls"));
    assertThat(shouldRead).as("Should skip: non-null column contains no null values").isFalse();

    shouldRead = shouldRead(isNull("map_not_null"));
    assertThat(shouldRead).as("Should read: map type is not skipped").isTrue();

    shouldRead = shouldRead(isNull("struct_not_null"));
    assertThat(shouldRead).as("Should read: struct type is not skipped").isTrue();
  }

  @Test
  public void testFloatWithNan() {
    // NaN's should break Parquet's Min/Max stats we should be reading in all cases
    boolean shouldRead = shouldRead(greaterThan("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(lessThan("some_nans", 3.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(lessThanOrEqual("some_nans", 1.0));
    assertThat(shouldRead).isTrue();

    shouldRead = shouldRead(equal("some_nans", 2.0));
    assertThat(shouldRead).isTrue();
  }

  @Test
  public void testDoubleWithNan() {
    boolean shouldRead = shouldRead(greaterThan("some_double_nans", 1.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("some_double_nans", 1.0));
    assertThat(shouldRead)
        .as("Should read: column with some nans contains the target value")
        .isTrue();

    shouldRead = shouldRead(lessThan("some_double_nans", 3.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("some_double_nans", 1.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();

    shouldRead = shouldRead(equal("some_double_nans", 2.0));
    assertThat(shouldRead).as("Should read: column with some nans contains target value").isTrue();
  }

  @Test
  public void testNotNaN() {
    boolean shouldRead = shouldRead(notNaN("all_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(notNaN("some_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(notNaN("no_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(notNaN("all_nulls"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();
  }

  @Test
  public void testRequiredColumn() {
    boolean shouldRead = shouldRead(notNull("required"));
    assertThat(shouldRead).as("Should read: required columns are always non-null").isTrue();

    shouldRead = shouldRead(isNull("required"));
    assertThat(shouldRead).as("Should skip: required columns are always non-null").isFalse();
  }

  @Test
  public void testMissingColumn() {
    assertThatThrownBy(() -> shouldRead(lessThan("missing", 5)))
        .as("Should complain about missing column in expression")
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'missing'");
  }

  @Test
  public void testNot() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead = shouldRead(not(lessThan("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should read: not(false)").isTrue();

    shouldRead = shouldRead(not(greaterThan("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should skip: not(true)").isFalse();
  }

  @Test
  public void testAnd() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MIN_VALUE - 30)));
    assertThat(shouldRead).as("Should skip: and(false, true)").isFalse();

    shouldRead =
        shouldRead(
            and(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should skip: and(false, false)").isFalse();

    shouldRead =
        shouldRead(
            and(greaterThan("id", INT_MIN_VALUE - 25), lessThanOrEqual("id", INT_MIN_VALUE)));
    assertThat(shouldRead).as("Should read: and(true, true)").isTrue();
  }

  @Test
  public void testOr() {
    // this test case must use a real predicate, not alwaysTrue(), or binding will simplify it out
    boolean shouldRead =
        shouldRead(
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should skip: or(false, false)").isFalse();

    shouldRead =
        shouldRead(
            or(lessThan("id", INT_MIN_VALUE - 25), greaterThanOrEqual("id", INT_MAX_VALUE - 19)));
    assertThat(shouldRead).as("Should read: or(false, true)").isTrue();
  }

  @Test
  public void testIntegerLt() {
    boolean shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    shouldRead = shouldRead(lessThan("id", INT_MIN_VALUE + 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThan("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerLtEq() {
    boolean shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testIntegerGt() {
    boolean shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThan("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerGtEq() {
    boolean shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testIntegerEq() {
    boolean shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead = shouldRead(equal("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @Test
  public void testIntegerNotEq() {
    boolean shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testIntegerNotEqRewritten() {
    boolean shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 25)));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE - 1)));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MIN_VALUE)));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE - 4)));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE)));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 1)));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(not(equal("id", INT_MAX_VALUE + 6)));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testStructFieldLt() {
    boolean shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range below lower bound (30 is not < 30)")
        .isFalse();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MIN_VALUE + 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThan("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testStructFieldLtEq() {
    boolean shouldRead =
        shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id range below lower bound (5 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id range below lower bound (29 < 30)").isFalse();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(lessThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: many possible ids").isTrue();
  }

  @Test
  public void testStructFieldGt() {
    boolean shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead)
        .as("Should not read: id range above upper bound (79 is not > 79)")
        .isFalse();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 1));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThan("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testStructFieldGtEq() {
    boolean shouldRead =
        shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id range above upper bound (85 < 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id range above upper bound (80 > 79)").isFalse();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: one possible id").isTrue();

    shouldRead = shouldRead(greaterThanOrEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: may possible ids").isTrue();
  }

  @Test
  public void testStructFieldEq() {
    boolean shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();

    shouldRead = shouldRead(equal("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should not read: id above upper bound").isFalse();
  }

  @Test
  public void testStructFieldNotEq() {
    boolean shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 25));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE - 4));
    assertThat(shouldRead).as("Should read: id between lower and upper bounds").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE));
    assertThat(shouldRead).as("Should read: id equal to upper bound").isTrue();

    shouldRead = shouldRead(notEqual("id", INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();

    shouldRead = shouldRead(notEqual("struct_not_null.int_field", INT_MAX_VALUE + 6));
    assertThat(shouldRead).as("Should read: id above upper bound").isTrue();
  }

  @Test
  public void testCaseInsensitive() {
    boolean shouldRead = shouldRead(equal("ID", INT_MIN_VALUE - 25), false);
    assertThat(shouldRead).as("Should not read: id below lower bound").isFalse();
  }

  @Test
  public void testIntegerIn() {
    boolean shouldRead = shouldRead(in("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    assertThat(shouldRead).as("Should not read: id below lower bound (5 < 30, 6 < 30)").isFalse();

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should not read: id below lower bound (28 < 30, 29 < 30)").isFalse();

    shouldRead = shouldRead(in("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    assertThat(shouldRead).as("Should not read: id above upper bound (80 > 79, 81 > 79)").isFalse();

    shouldRead = shouldRead(in("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    assertThat(shouldRead).as("Should not read: id above upper bound (85 > 79, 86 > 79)").isFalse();

    shouldRead = shouldRead(in("all_nulls", 1, 2));
    assertThat(shouldRead).as("Should skip: in on all nulls column").isFalse();

    shouldRead = shouldRead(in("some_nulls", "aaa", "some"));
    assertThat(shouldRead).as("Should read: in on some nulls column").isTrue();

    shouldRead = shouldRead(in("no_nulls", "aaa", ""));
    assertThat(shouldRead).as("Should read: in on no nulls column").isTrue();
  }

  protected void testIntegerNotIn() {
    boolean shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 25, INT_MIN_VALUE - 24));
    assertThat(shouldRead).as("Should read: id below lower bound (5 < 30, 6 < 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 2, INT_MIN_VALUE - 1));
    assertThat(shouldRead).as("Should read: id below lower bound (28 < 30, 29 < 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MIN_VALUE - 1, INT_MIN_VALUE));
    assertThat(shouldRead).as("Should read: id equal to lower bound (30 == 30)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE - 4, INT_MAX_VALUE - 3));
    assertThat(shouldRead)
        .as("Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)")
        .isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE, INT_MAX_VALUE + 1));
    assertThat(shouldRead).as("Should read: id equal to upper bound (79 == 79)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 1, INT_MAX_VALUE + 2));
    assertThat(shouldRead).as("Should read: id above upper bound (80 > 79, 81 > 79)").isTrue();

    shouldRead = shouldRead(notIn("id", INT_MAX_VALUE + 6, INT_MAX_VALUE + 7));
    assertThat(shouldRead).as("Should read: id above upper bound (85 > 79, 86 > 79)").isTrue();

    shouldRead = shouldRead(notIn("all_nulls", 1, 2));
    assertThat(shouldRead).as("Should read: notIn on all nulls column").isTrue();

    shouldRead = shouldRead(notIn("some_nulls", "aaa", "some"));
    assertThat(shouldRead).as("Should read: notIn on some nulls column").isTrue();
  }

  @Test
  public void testSomeNullsNotEq() {
    boolean shouldRead = shouldRead(notEqual("some_nulls", "some"));
    assertThat(shouldRead).as("Should read: notEqual on some nulls column").isTrue();
  }

  protected boolean supportsVariant() {
    return true;
  }

  @Test
  public void testVariantNotNull() {
    assumeThat(supportsVariant()).isTrue();

    boolean shouldRead = shouldRead(notNull("some_nulls_variant"));
    assertThat(shouldRead)
        .as("Should read: variant notNull filters must be evaluated post scan")
        .isTrue();

    shouldRead = shouldRead(notNull("all_nulls_variant"));
    assertThat(shouldRead)
        .as("Should read: variant notNull filters must be evaluated post scan for all nulls")
        .isTrue();
  }

  @Test
  public void testVariantEq() {
    assumeThat(supportsVariant()).isTrue();

    assertThatThrownBy(() -> shouldRead(equal("some_nulls_variant", "test")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test");
  }

  @Test
  public void testVariantIn() {
    assumeThat(supportsVariant()).isTrue();

    assertThatThrownBy(() -> shouldRead(in("some_nulls_variant", "test1", "test2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test1");

    assertThatThrownBy(() -> shouldRead(in("all_nulls_variant", "test1", "test2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test1");
  }

  @Test
  public void testVariantNotIn() {
    assumeThat(supportsVariant()).isTrue();

    // Variant columns cannot be used in 'notIn' expressions with literals
    assertThatThrownBy(() -> shouldRead(notIn("some_nulls_variant", "test1", "test2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test1");

    assertThatThrownBy(() -> shouldRead(notIn("all_nulls_variant", "test1", "test2")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("test1");
  }

  @Test
  public void testVariantIsNull() {
    assumeThat(supportsVariant()).isTrue();

    boolean shouldRead = shouldRead(isNull("some_nulls_variant"));
    assertThat(shouldRead)
        .as("Should read: variant isNull filters must be evaluated post scan")
        .isTrue();

    shouldRead = shouldRead(isNull("all_nulls_variant"));
    assertThat(shouldRead)
        .as("Should read: variant isNull filters must be evaluated post scan even for all nulls")
        .isTrue();
  }

  @Test
  public void testVariantComparisons() {
    assumeThat(supportsVariant()).isTrue();

    // Variant columns cannot be used in comparison expressions with literals
    assertThatThrownBy(() -> shouldRead(lessThan("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");

    assertThatThrownBy(() -> shouldRead(lessThanOrEqual("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");

    assertThatThrownBy(() -> shouldRead(greaterThan("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");

    assertThatThrownBy(() -> shouldRead(greaterThanOrEqual("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");

    assertThatThrownBy(() -> shouldRead(notEqual("some_nulls_variant", 42)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type variant:")
        .hasMessageContaining("42");
  }

  protected abstract boolean shouldRead(Expression expression, boolean caseSensitive);

  protected boolean shouldRead(Expression expression) {
    return shouldRead(expression, true);
  }

  protected Schema schema() {
    if (supportsVariant()) {
      return TypeUtil.join(
          SCHEMA,
          new Schema(
              optional(18, "some_nulls_variant", Types.VariantType.get()),
              optional(19, "all_nulls_variant", Types.VariantType.get())));
    }

    return SCHEMA;
  }
}
