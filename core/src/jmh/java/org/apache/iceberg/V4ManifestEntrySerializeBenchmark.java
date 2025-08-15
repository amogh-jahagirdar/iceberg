///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.iceberg;
//
//import static java.lang.String.format;
//
//import com.google.common.base.Stopwatch;
//import java.io.File;
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//import java.util.concurrent.TimeUnit;
//import java.util.function.Supplier;
//import org.apache.commons.io.FileUtils;
//import org.apache.iceberg.inmemory.InMemoryOutputFile;
//import org.apache.iceberg.io.OutputFile;
//import org.apache.iceberg.relocated.com.google.common.collect.Lists;
//import org.openjdk.jmh.annotations.Benchmark;
//import org.openjdk.jmh.annotations.BenchmarkMode;
//import org.openjdk.jmh.annotations.Fork;
//import org.openjdk.jmh.annotations.Measurement;
//import org.openjdk.jmh.annotations.Mode;
//import org.openjdk.jmh.annotations.Param;
//import org.openjdk.jmh.annotations.Scope;
//import org.openjdk.jmh.annotations.Setup;
//import org.openjdk.jmh.annotations.State;
//import org.openjdk.jmh.annotations.TearDown;
//import org.openjdk.jmh.annotations.Threads;
//import org.openjdk.jmh.annotations.Timeout;
//import org.openjdk.jmh.annotations.Warmup;
//
///**
// * A benchmark that evaluates the performance of writing manifest files
// *
// * <p>To run this benchmark: <code>
// *   ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestV4WriteBenchmark
// * </code>
// */
//@Fork(0)
//@State(Scope.Benchmark)
//@Measurement(iterations = 2)
//@BenchmarkMode(Mode.AverageTime)
//@Warmup(iterations = 2)
//@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
//public class V4ManifestEntrySerializeBenchmark {
//  private static final int PREGENERATE_ENTRIES = 20000;
//
//  private static String baseDir = "/tmp/v4/single_write_benchmark";
//
//  Schema schema = V4BenchmarkSchemas.SCHEMA_16;
//  Map<Integer, Supplier<Object>> statsSupplier = V4BenchmarkSchemas.SCHEMA_16_STATS_SUPPLIER;
//  V4MetadataSimbenchUtil simbenchUtil = new V4MetadataSimbenchUtil(schema, statsSupplier);
//
//  List<V4ManifestEntry> entries = Lists.newArrayListWithCapacity(PREGENERATE_ENTRIES);
//
//  @Setup
//  public void before() throws IOException {
//    // Disable noisy log
//    System.setProperty("org.slf4j.simpleLogger.log.org.apache.hadoop.io.compress.CodecPool", "off");
//
//    FileUtils.deleteQuietly(new File(baseDir));
//    FileUtils.createParentDirectories(new File(baseDir));
//
//    System.out.println("Preparing Entries");
//    Stopwatch stopwatch = Stopwatch.createStarted();
//    pregenerateEntries();
//    stopwatch.stop();
//    System.out.printf(
//        "Generated %s entries in %s ms%n",
//        entries.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
//  }
//
//  @TearDown
//  public void after() {
//    // FileUtils.deleteQuietly(new File(baseDir));
//  }
//
//  @State(Scope.Benchmark)
//  public static class BenchmarkState {
//    // @Param({"PARQUET", "AVRO"})
//    @Param({"PARQUET"})
//    FileFormat format;
//
//    // @Param({"MEMORY", "DISK"})
//    @Param({"MEMORY"})
//    String outputSource;
//
//    OutputFile getOutputFile() {
//      switch (outputSource) {
//        case "MEMORY":
//          return new InMemoryOutputFile();
//        case "DISK":
//          return Files.localOutput(baseDir + format.addExtension("/v4-manifest"));
//        default:
//          throw new UnsupportedOperationException("Unsupported output source: " + outputSource);
//      }
//    }
//  }
//
//  // @Benchmark
//  @Threads(1)
//  public void v4_1_entries(BenchmarkState state) throws IOException {
//    writeV4Manifest(state, 1);
//  }
//
//  // @Benchmark
//  @Threads(1)
//  public void v4_10_entries(BenchmarkState state) throws IOException {
//    writeV4Manifest(state, 10);
//  }
//
//  // @Benchmark
//  @Threads(1)
//  public void v4_100_entries(BenchmarkState state) throws IOException {
//    writeV4Manifest(state, 100);
//  }
//
//  @Benchmark
//  @Threads(1)
//  public void v4_1K_entries(BenchmarkState state) throws IOException {
//    writeV4Manifest(state, 1000);
//  }
//
//  // @Benchmark
//  @Threads(1)
//  public void v4_10K_entries(BenchmarkState state) throws IOException {
//    writeV4Manifest(state, 10000);
//  }
//
//  private void writeV4Manifest(BenchmarkState state, int entriesToWrite) throws IOException {
//    OutputFile outputFile = state.getOutputFile();
//
//    try (V4Metadata2.Writer writer =
//        new V4Metadata2.Writer(outputFile, state.format, schema, 2, 1L, 1, 0L)) {
//
//      entries.subList(0, entriesToWrite).forEach(writer::add);
//    }
//  }
//
//  private void pregenerateEntries() {
//    for (int i = 0; i < PREGENERATE_ENTRIES; i++) {
//      V4ManifestEntry.EntryV4 entry =
//          simbenchUtil.generateDataFileEntry(i, UUID.randomUUID().toString(), i);
//      entries.add(entry);
//    }
//  }
//}
