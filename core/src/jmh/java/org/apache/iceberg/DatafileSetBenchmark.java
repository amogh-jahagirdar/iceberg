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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jol.info.GraphLayout;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Timeout(time = 20, timeUnit = TimeUnit.MINUTES)
@BenchmarkMode(Mode.SingleShotTime)
public class DatafileSetBenchmark {

    static class ObjectSizeFetcher {
        private static Instrumentation instrumentation;

        public static void premain(String args, Instrumentation inst) {
            instrumentation = inst;
        }

        public static long getObjectSize(Object o) {
            return instrumentation.getObjectSize(o);
        }
    }

    private static final int NUM_PARTITIONS = 50;
    private static final int NUM_DATA_FILES_PER_PARTITION = 900000;

    private static final int NUM_COLS = 10;

    @Param({"false", "true"})
    private boolean benchmarkHashSet;

    private Set<DataFile> dataFiles;

    @Setup
    public void before() {
        Map<Integer, PartitionSpec> specs = Maps.newHashMap();
        specs.put(0, PartitionSpec.unpartitioned());
        if (benchmarkHashSet) {
            dataFiles = new HashSet<>();
        } else {
            dataFiles = new DatafileSet(specs);
        }
    }

    @Benchmark
    @Threads(1)
    public void benchmarkSetAdds() throws IOException {
        before();
        Random random = new Random(System.currentTimeMillis());
        Metrics metrics = randomMetrics(random);
        for (int i = 0; i < NUM_DATA_FILES_PER_PARTITION; i++) {
            DataFile dataFile =
                    DataFiles.builder(PartitionSpec.unpartitioned())
                            .withFormat(FileFormat.PARQUET)
                            .withPath(String.format("s3://some-warehouse-id/some-database-id/some-table-id/%s.parquet", i))
                            .withFileSizeInBytes(100)
                            .withRecordCount(100)
                            .withMetrics(metrics)
                            .build();
            dataFiles.add(dataFile);
        }

        long size = GraphLayout.parseInstance(dataFiles).totalSize();
        System.out.printf("Memory used by %s is %d%n", benchmarkHashSet ? "hashset" : "trie", size);
        dataFiles.clear();
    }

    private Metrics randomMetrics(Random random) {
        long rowCount = 100000L + random.nextInt(1000);
        Map<Integer, Long> columnSizes = org.apache.iceberg.relocated.com.google.common.collect.Maps.newHashMap();
        Map<Integer, Long> valueCounts = org.apache.iceberg.relocated.com.google.common.collect.Maps.newHashMap();
        Map<Integer, Long> nullValueCounts = org.apache.iceberg.relocated.com.google.common.collect.Maps.newHashMap();
        Map<Integer, Long> nanValueCounts = org.apache.iceberg.relocated.com.google.common.collect.Maps.newHashMap();
        Map<Integer, ByteBuffer> lowerBounds = org.apache.iceberg.relocated.com.google.common.collect.Maps.newHashMap();
        Map<Integer, ByteBuffer> upperBounds = org.apache.iceberg.relocated.com.google.common.collect.Maps.newHashMap();
        for (int i = 0; i < NUM_COLS; i++) {
            columnSizes.put(i, 1000000L + random.nextInt(100000));
            valueCounts.put(i, 100000L + random.nextInt(100));
            nullValueCounts.put(i, (long) random.nextInt(5));
            nanValueCounts.put(i, (long) random.nextInt(5));
            byte[] lower = new byte[8];
            random.nextBytes(lower);
            lowerBounds.put(i, ByteBuffer.wrap(lower));
            byte[] upper = new byte[8];
            random.nextBytes(upper);
            upperBounds.put(i, ByteBuffer.wrap(upper));
        }

        return new Metrics(
                rowCount,
                columnSizes,
                valueCounts,
                nullValueCounts,
                nanValueCounts,
                lowerBounds,
                upperBounds);
    }
}
