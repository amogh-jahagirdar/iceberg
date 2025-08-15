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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UnicodeUtil;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class V4BenchmarkSchemas {
//  private static final Random RANDOM = new Random(System.currentTimeMillis());

  public static Schema SCHEMA_8 = new Schema(
          optional(1, "metric", Types.StringType.get()),
          optional(2, "ts", Types.TimestampType.withoutZone()),
          optional(3, "source", Types.StringType.get()),
          optional(4, "region", Types.StringType.get()),
          optional(5, "value_1", Types.LongType.get()),
          optional(6, "value_2", Types.LongType.get()),
          optional(7, "value_3", Types.LongType.get()),
          optional(8, "value_4", Types.LongType.get()));

//  public static Map<Integer, Supplier<Object>> SCHEMA_8_STATS_SUPPLIER =
//          new HashMap<>() {
//            {
//              put(1, () -> randomOf(List.of("Create", "Update", "Delete")));
//              put(2, () -> randomOf(List.of(Instant.now().getEpochSecond())));
//              put(3, () -> randomOf(List.of("application", "service", "client", "log")));
//              put(4, () ->
//                              randomOf(
//                                      List.of(
//                                              "us-east-1",
//                                              "us-west-2",
//                                              "eu-west-1",
//                                              "ap-southeast-1",
//                                              "us-west-1",
//                                              "eu-central-1",
//                                              "ap-northeast-1",
//                                              "us-east-2")));
//              put(5, RANDOM::nextLong);
//              put(6, RANDOM::nextLong);
//              put(7, RANDOM::nextLong);
//              put(8, RANDOM::nextLong);
//            }
//          };

  public static Schema SCHEMA_16 =
          new Schema(
                  optional(1, "event_type", Types.StringType.get()),
                  optional(2, "source", Types.StringType.get()),
                  optional(3, "region", Types.StringType.get()),
                  optional(4, "status", Types.StringType.get()),
                  optional(5, "payload", Types.StringType.get()),
                  optional(6, "account_id", Types.LongType.get()),
                  optional(7, "subscriber_id", Types.LongType.get()),
                  optional(8, "post_count", Types.IntegerType.get()),
                  optional(9, "reply_count", Types.IntegerType.get()),
                  optional(10, "balance", Types.DoubleType.get()),
                  optional(11, "last_txn", Types.FloatType.get()),
                  optional(12, "joined_at", Types.DateType.get()),
                  optional(13, "is_test", Types.BooleanType.get()),
                  optional(14, "is_active", Types.BooleanType.get()),
                  optional(15, "event_ts", Types.TimestampType.withoutZone()),
                  optional(16, "arrival_ts", Types.TimestampType.withoutZone()));

//  public static Map<Integer, Supplier<Object>> SCHEMA_16_STATS_SUPPLIER =
//          new HashMap<>() {
//            {
//              put(1, () -> randomOf(List.of("Create", "Update", "Delete")));
//              put(2, () -> randomOf(List.of("application", "service", "client", "log")));
//              put(
//                      3,
//                      () ->
//                              randomOf(
//                                      List.of(
//                                              "us-east-1",
//                                              "us-west-2",
//                                              "eu-west-1",
//                                              "ap-southeast-1",
//                                              "us-west-1",
//                                              "eu-central-1",
//                                              "ap-northeast-1",
//                                              "us-east-2")));
//              put(
//                      4,
//                      () ->
//                              randomOf(
//                                      List.of(
//                                              "active",
//                                              "inactive",
//                                              "initializing",
//                                              "on-hold",
//                                              "degraded",
//                                              "unknown")));
//              put(5, () -> UnicodeUtil.truncateString(RandomStringUtils.randomAlphanumeric(32), 16));
//              put(6, RANDOM::nextLong);
//              put(7, RANDOM::nextLong);
//              put(8, () -> RANDOM.nextInt(1000));
//              put(9, () -> RANDOM.nextInt(1000));
//              put(10, RANDOM::nextDouble);
//              put(11, RANDOM::nextFloat);
//              put(12, () -> RANDOM.nextInt(1000));
//              put(13, RANDOM::nextBoolean);
//              put(14, RANDOM::nextBoolean);
//              put(15, () -> randomOf(List.of(Instant.now().getEpochSecond())));
//              put(16, () -> randomOf(List.of(Instant.now().getEpochSecond())));
//            }
//          };
//
//  public static Schema SCHEMA_DEGEN_32 =
//      new Schema(
//          optional(1, "s1", Types.StringType.get()),
//          optional(2, "s2", Types.StringType.get()),
//          optional(3, "s3", Types.StringType.get()),
//          optional(4, "s4", Types.StringType.get()),
//          optional(5, "s5", Types.StringType.get()),
//          optional(6, "s6", Types.StringType.get()),
//          optional(7, "s7", Types.StringType.get()),
//          optional(8, "s8", Types.StringType.get()),
//          optional(9, "s9", Types.StringType.get()),
//          optional(10, "s10", Types.StringType.get()),
//          optional(11, "s11", Types.StringType.get()),
//          optional(12, "s12", Types.StringType.get()),
//          optional(13, "s13", Types.StringType.get()),
//          optional(14, "s14", Types.StringType.get()),
//          optional(15, "s15", Types.StringType.get()),
//          optional(16, "s16", Types.StringType.get()),
//          optional(17, "s17", Types.StringType.get()),
//          optional(18, "s18", Types.StringType.get()),
//          optional(19, "s19", Types.StringType.get()),
//          optional(20, "s20", Types.StringType.get()),
//          optional(21, "s21", Types.StringType.get()),
//          optional(22, "s22", Types.StringType.get()),
//          optional(23, "s23", Types.StringType.get()),
//          optional(24, "s24", Types.StringType.get()),
//          optional(25, "s25", Types.StringType.get()),
//          optional(26, "s26", Types.StringType.get()),
//          optional(27, "s27", Types.StringType.get()),
//          optional(28, "s28", Types.StringType.get()),
//          optional(29, "s29", Types.StringType.get()),
//          optional(30, "s30", Types.StringType.get()),
//          optional(31, "s31", Types.StringType.get()),
//          optional(32, "s32", Types.StringType.get()));

//  public static Map<Integer, Supplier<Object>> SCHEMA_32_DEGEN_STATS_SUPPLIER;
//
//  static {
//    Map<Integer, Supplier<Object>> statsSupplier = new HashMap<>();
//    IntStream.range(1, 33)
//        .forEach(
//            i ->
//                statsSupplier.put(
//                    i,
//                    () ->
//                        UnicodeUtil.truncateString(RandomStringUtils.randomAlphanumeric(32), 16)));
//
//    SCHEMA_32_DEGEN_STATS_SUPPLIER = statsSupplier;
//  }

//  private static Object randomOf(List<?> values) {
//    return values.get(RANDOM.nextInt(values.size()));
//  }
//
//  public static void main(String[] args) {
//    // System.out.println(SchemaParser.toJson(V4Metadata2.manifestSchema(SCHEMA_DEGEN_32),true));
//    System.out.println(SchemaParser.toJson(SCHEMA_DEGEN_32, true));
//  }
}
