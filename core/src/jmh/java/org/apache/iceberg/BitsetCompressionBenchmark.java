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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.openjdk.jmh.annotations.AuxCounters;
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

/**
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=BitsetCompressionBenchmark
 * -PjmhOutputPath=benchmark/bitsetbenchmark.txt
 */
@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SingleShotTime)
public class BitsetCompressionBenchmark {

  @Param({"1000", "10000", "100000", "1000000"})
  private int totalBits;

  @Param({"RANDOM"})
  private SetMode mode;

  @Param({"0.01", "0.05", "0.1", "0.2"})
  private float percentageBitsToSet;

  private static final Random RANDOM = new Random(10);

  private Set<Integer> positions;

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.EVENTS)
  public static class CompressedSize {
    public long numBytes;
  }

  @Setup
  public void setupBenchmark() {
    // Generate the bit positions
    this.positions = generateBitPositions(totalBits, (int) (percentageBitsToSet * totalBits), mode);
  }

  @Benchmark
  @Threads(1)
  public void testBitsetCompressionRoaringBitmap(CompressedSize size) throws IOException {
    RoaringBitmap bitmap = new RoaringBitmap();
    positions.forEach(bitmap::add);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    bitmap.runOptimize();
    bitmap.serialize(dos);
    dos.close();
    size.numBytes += bos.size();
  }

  @Benchmark
  @Threads(1)
  public void testBitsetCompressionLZ4(CompressedSize size) throws IOException {
    testBitsetCompression(
        size,
        (OutputStream out) -> {
          CompressionCodec codec =
              new CompressionCodecFactory(new Configuration()).getCodecByName("lz4");
          try {
            return codec.createOutputStream(out);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Benchmark
  @Threads(1)
  public void testBitsetCompressionZstd(CompressedSize size) throws IOException {
    testBitsetCompression(
        size,
        (OutputStream out) -> {
          try {
            return new ZstdCompressorOutputStream(out);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Benchmark
  @Threads(1)
  public void testBitsetCompressionRLE(CompressedSize size) throws IOException {
    BitSet bitset = new BitSet(totalBits);
    for (int position : positions) {
      bitset.set(position);
    }

    List<Integer> rle = runLengthEncodeBitSet(bitset, totalBits);
    ByteBuffer buffer = ByteBuffer.allocate(rle.size() * 4);
    for (int pos : rle) {
      buffer.putInt(pos);
    }

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    bout.writeBytes(buffer.array());
    bout.close();
    size.numBytes += bout.size();
  }

  @Benchmark
  @Threads(1)
  public void testSerializeRawBitset(CompressedSize size) throws IOException {
    BitSet bitset = new BitSet(totalBits);
    for (int position : positions) {
      bitset.set(position);
    }

    byte[] densePacked = bitset.toByteArray();
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    bout.writeBytes(densePacked);
    bout.close();
    size.numBytes += bout.size();
  }

  private void testBitsetCompression(
      CompressedSize size, Function<OutputStream, OutputStream> compressor) throws IOException {
    BitSet bitset = new BitSet(totalBits);
    for (int position : positions) {
      bitset.set(position);
    }

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    OutputStream compressedOut = compressor.apply(bout);
    compressedOut.write(bitset.toByteArray());
    compressedOut.close();
    bout.close();
    size.numBytes += bout.size();
  }

  // Mode for set bits generation
  public enum SetMode {
    RANDOM
  }

  // Generalized bit position generator
  private Set<Integer> generateBitPositions(int bitCount, int n, SetMode mode) {
    Set<Integer> positions = new HashSet<>();

    switch (mode) {
      case RANDOM:
        while (positions.size() < n) {
          positions.add(RANDOM.nextInt(bitCount));
        }
        break;
    }
    return positions;
  }

  private static List<Integer> runLengthEncodeBitSet(BitSet bitSet, int bits) {
    List<Integer> runs = new ArrayList<>();
    boolean currentBit = false; // start with 0s
    int runLength = 0;

    for (int i = 0; i < bits; i++) {
      boolean bit = bitSet.get(i);
      if (bit == currentBit) {
        runLength++;
      } else {
        runs.add(runLength);
        runLength = 1;
        currentBit = !currentBit;
      }
    }
    runs.add(runLength); // add the final run
    return runs;
  }
}
