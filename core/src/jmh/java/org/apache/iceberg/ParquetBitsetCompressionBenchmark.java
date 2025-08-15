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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
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
import org.roaringbitmap.RoaringBitmap;

import static org.apache.iceberg.V4BenchmarkSchemas.SCHEMA_16;
import static org.apache.iceberg.types.Types.NestedField.optional;

/**
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ParquetBitsetCompressionBenchmark
 * -PjmhOutputPath=benchmark/parquet-bitsetbenchmark.txt
 */
@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SingleShotTime)
public class ParquetBitsetCompressionBenchmark {
    @Param({"1000", "2500", "5000", "10000", "100000"})
    private int totalBits;

    @Param({"RANDOM"})
    private BitsetCompressionBenchmark.SetMode mode;

    @Param({"0.01", "0.05", "0.1", "0.2"})
    private float percentageBitsToSet;

    @Param({"100", "1000", "2500", "5000"})
    private int numberManifestDvs;

    private final String baseDir = "/tmp/simbench/iceberg/";

    private List<ByteBuffer> serializedDvs;

    @Setup
    public void setupBenchmark() throws IOException {
        this.serializedDvs = Lists.newArrayList();
        // Pregenerate all the entries
        for (int i = 0; i < numberManifestDvs; i++) {
            // Make every record have random bit positions
            Set<Integer> positions = generateBitPositions(totalBits, (int) (percentageBitsToSet * totalBits), mode, new Random());
            RoaringBitmap bitmap = new RoaringBitmap();
            positions.forEach(bitmap::add);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            bitmap.runOptimize();
            bitmap.serialize(dos);
            dos.close();
            serializedDvs.add(ByteBuffer.wrap(bos.toByteArray()));
        }
    }


    @Benchmark
    @Threads(1)
    public void testRoaringBitmapWithZstd(BitsetCompressionBenchmark.CompressedSize size) throws IOException {
        OutputFile outputFile = Files.localOutput(baseDir +
                String.format(Locale.getDefault(), "%d-manifestDVs-%.2f-density-%d-bits", numberManifestDvs, percentageBitsToSet, totalBits)+ "-root-manifest.parquet");
        Schema schema = new Schema(optional(1, "dv_content", Types.BinaryType.get()));
        FileAppender<Record> recordWriter = Parquet.write(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::create)
                .set("write.parquet.compression-codec", "zstd")
                .set("write.parquet.compression-level", "3")
                .set("parquet.column.statistics.enabled", "false")
                .set("write.parquet.column.statistics.enabled", "false")
                .overwrite().build();
        for (ByteBuffer dv : serializedDvs) {
            GenericRecord record = GenericRecord.create(schema);
            record.set(0, dv);
            recordWriter.add(record);
        }

        recordWriter.close();
    }


    // Generalized bit position generator
    private Set<Integer> generateBitPositions(int bitCount, int n, BitsetCompressionBenchmark.SetMode mode, Random random) {
        Set<Integer> positions = new HashSet<>();

        switch (mode) {
            case RANDOM:
                while (positions.size() < n) {
                    positions.add(random.nextInt(bitCount));
                }
                break;
        }
        return positions;
    }
}
