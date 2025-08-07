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


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;

import static java.lang.String.format;

/**
 * A benchmark that evaluates the performance of writing manifest files
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=V4ManifestSimulationBenchmark
 * </code>
 */
@Fork(0)
@State(Scope.Benchmark)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class V4ManifestSimulationBenchmark {
  private static final int PREGENERATED_ENTRIES = 20000;

  private final List<V4ManifestEntry> pregeneratedEntries = Lists.newArrayListWithCapacity(PREGENERATED_ENTRIES);

  private final String baseDir = "/tmp/simbench/iceberg/";
  private String rootManifestFile;

  private BufferedWriter outputWriter;
  private ObjectMapper objectMapper;

  @Setup
  public void before(BenchmarkState state) throws IOException {
    // Disable noisy log
    System.setProperty("org.slf4j.simpleLogger.log.org.apache.hadoop.io.compress.CodecPool", "off");

    FileUtils.deleteQuietly(new File(baseDir));
    FileUtils.createParentDirectories(new File(baseDir));
    outputWriter = new BufferedWriter(new FileWriter("/tmp/simbench/iceberg-data.json"));
    objectMapper = new ObjectMapper();

    System.out.println("Preparing Entries");
    Stopwatch stopwatch = Stopwatch.createStarted();
    pregenerateEntries(state);
    stopwatch.stop();
    System.out.printf("Generated %s entries in %s ms%n", pregeneratedEntries.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }

  @TearDown
  public void after() {
    IOUtils.closeQuietly(outputWriter);
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    //@Param({"MEMORY", "DISK"})
    @Param({"DISK"})
    String outputSource;

    //@Param({"PARQUET", "AVRO"})
    @Param({"PARQUET"})
    FileFormat format;

    OutputFile getOutputFile(String location) {
      switch (outputSource) {
        case "MEMORY": return new InMemoryOutputFile(location);
        case "DISK": return Files.localOutput(location);
        default: throw new UnsupportedOperationException("Unsupported output source: " + outputSource);
      }
    }

    Random random = new Random();
    MetricsRecord metricsRecord = new MetricsRecord();
    List<V4ManifestEntry> dataFiles = Lists.newArrayList();;
    Map<V4ManifestEntry, List<V4ManifestEntry>> manifests = Maps.newHashMap();

    int rebalanceThreshold = 20;
    int targetRootManifests = 20;
    int targetEntriesPerLeafManifest = 50_000;

    Schema schema = V4BenchmarkSchemas.SCHEMA_16;
    Map<Integer, Supplier<Object>> statsSupplier = V4BenchmarkSchemas.SCHEMA_16_STATS_SUPPLIER;
    V4MetadataSimbenchUtil simbenchUtil = new V4MetadataSimbenchUtil(schema, statsSupplier);

    long sequenceNumber = 0;
    long rebalanceCount = 0;
    long rebalanceTotalSize = 0;
    long currentRootSize = 0;
    long totalWriteSize = 0;

    MetricsRecord report() {
      return metricsRecord;
    }
    // Workaround to avoid JMH injected fields with using ObjectMapper to report state
    class MetricsRecord {
      @JsonProperty
      int leafManifests() {
        return manifests.size();
      }

      @JsonProperty
      int rootDataFiles() {
        return dataFiles.size();
      }

      @JsonProperty
      int rootEntries() {
        return dataFiles.size() + manifests.size();
      }

      @JsonProperty
      long sequenceNumber() {
        return sequenceNumber;
      };

      @JsonProperty
      long rebalanceCount() {
        return rebalanceCount;
      };

      @JsonProperty
      long rebalanceTotalSize() {
        return rebalanceTotalSize;
      };

      @JsonProperty
      long currentRootSize() {
        return currentRootSize;
      }

      @JsonProperty
      long totalWriteSize() { return totalWriteSize; }
    }
  }

  //@Benchmark
  @Threads(1)
  public void v4_1f_10080c(BenchmarkState state) throws IOException {
    writeV4Commits(state, 10, 10080);
  }

  @Benchmark
  @Threads(1)
  public void v4_10000f_1c(BenchmarkState state) throws IOException {
    writeV4Commits(state, 10000, 1);
  }


  private void writeV4Commits(BenchmarkState state, int filesPerCommit, int commitCount)
      throws IOException {
    String tableLocation = format("%s/%s-%s", baseDir, state.format.name().toLowerCase(Locale.ROOT), System.currentTimeMillis());
    
    for (int i = 0; i < commitCount; i++) {
      if (i % 100 == 0) {
        System.out.printf("Commit %d with %d manifests and %d entries\n", i, state.manifests.size(), state.dataFiles.size());
      }

      String writeUUID = UUID.randomUUID().toString();
      state.sequenceNumber++;
      this.rootManifestFile =
          state
              .format
              .addExtension(
                  format("%s/v%08d-r-%s", tableLocation, state.sequenceNumber, writeUUID));

      OutputFile outputFile = state.getOutputFile(rootManifestFile);

      try (V4Metadata2.Writer writer =
          new V4Metadata2.Writer(outputFile,
              state.format,
              state.schema,
              state.sequenceNumber,
              state.sequenceNumber-1,
              state.sequenceNumber,
              0L)) {

        prepareEntries(state, filesPerCommit, writeUUID);

        if (state.dataFiles.size() >= state.rebalanceThreshold) {
          rebalance(state, tableLocation, writeUUID);
        }

        state.manifests.keySet().forEach(writer::add);
        state.dataFiles.forEach(writer::add);
      }

      state.currentRootSize = outputFile.toInputFile().getLength();
      state.totalWriteSize += state.currentRootSize;

      outputWriter.write(objectMapper.writeValueAsString(state.report()));
      outputWriter.newLine();
    }
  }

  private void pregenerateEntries(BenchmarkState state) throws IOException {
    for (int i = 0; i < PREGENERATED_ENTRIES; i++) {
      V4ManifestEntry.EntryV4 entry = state.simbenchUtil.generateDataFileEntry(i, UUID.randomUUID().toString(), i);
      pregeneratedEntries.add(entry);
    }
  }

  private void prepareEntries(BenchmarkState state, int count, String writeUUID) {
    long sequenceNumber = state.sequenceNumber;

    int pos = state.random.nextInt(pregeneratedEntries.size()-count);
    List<V4ManifestEntry> entries = pregeneratedEntries.subList(pos, pos+count);

    for (int i = 0; i < entries.size(); i++) {
      V4ManifestEntry.EntryV4 entry = (V4ManifestEntry.EntryV4) entries.get(i);
      entry.content().location(String.format("00000-%s-%s-0000-%s.parquet", sequenceNumber, writeUUID, i));
      entry.sequenceNumber(sequenceNumber);
    }

    entries.forEach(entry -> {

    });
    state.dataFiles.addAll(entries);
  }

  private void rebalance(BenchmarkState state, String tableLocation, String writeUUID) {
    // Check if we have more than the target number of root manifests
    if (state.manifests.size() > state.targetRootManifests) {
      System.out.println("Rebalancing . . .");
      Map<V4ManifestEntry, List<V4ManifestEntry>> toRewrite =  state.manifests.entrySet().stream()
              .filter(e -> e.getValue().size() < state.targetEntriesPerLeafManifest * 0.8)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      if (toRewrite.isEmpty()) {
        state.targetRootManifests = state.targetRootManifests + 10;
        System.out.printf("Expanding root manifest entries to %s\n", state.targetRootManifests);
      }

      System.out.printf("Rebalancing %s manifests\n", toRewrite.size());
      toRewrite.keySet().forEach(state.manifests::remove);

      List<V4ManifestEntry> entries = toRewrite.entrySet().stream().flatMap(e -> e.getValue().stream()).collect(Collectors.toList());
      Lists.partition(entries, state.targetEntriesPerLeafManifest)
          .parallelStream()
          .forEach(
              dataFileEntries -> {
                System.out.printf("Writing %s entries to manifest\n",  dataFileEntries.size());
                String manifestPath = state.format.addExtension(format("%s/00000-%s-%s-0000-m", tableLocation, state.sequenceNumber, writeUUID+"-rw-"));
                OutputFile manifestOutputFile = state.getOutputFile(manifestPath);

                V4ManifestEntry manifestEntry = state.simbenchUtil.generateManifest(dataFileEntries, state.sequenceNumber, manifestOutputFile);
                state.manifests.put(manifestEntry, dataFileEntries);
                state.rebalanceTotalSize += manifestEntry.content().fileSize();
              });
    }

    String manifestPath = state.format.addExtension(format("%s/00000-%s-%s-0000-m", tableLocation, state.sequenceNumber, writeUUID+"-rw-"));
    OutputFile manifestOutputFile = state.getOutputFile(manifestPath);
    V4ManifestEntry manifestEntry = state.simbenchUtil.generateManifest(state.dataFiles, state.sequenceNumber, manifestOutputFile);
    state.rebalanceCount++;
    state.rebalanceTotalSize += manifestEntry.content().fileSize();
    state.totalWriteSize += manifestEntry.content().fileSize();

    state.manifests.put(manifestEntry, state.dataFiles);
    state.dataFiles.clear();
  }
}
