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
package org.apache.iceberg.data.orc;

import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.TestMetricsRowGroupFilter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.junit.jupiter.api.Test;

public class TestOrcMetricsFilter extends TestMetricsRowGroupFilter {
  private File orcFile;

  @Override
  protected FileAppender<GenericData.Record> createFileAppender() throws IOException {
    this.orcFile = new File(tempDir, "junit" + System.nanoTime());

    OutputFile outFile = Files.localOutput(orcFile);
    FileAppender<GenericData.Record> appender =
            ORC.write(outFile)
                    .schema(FILE_SCHEMA)
                    .createWriterFunc(GenericOrcWriter::buildWriter)
                    .build();

    InputFile inFile = Files.localInput(orcFile);
    try (Reader reader =
        OrcFile.createReader(
            new Path(inFile.location()), OrcFile.readerOptions(new Configuration()))) {
      assertThat(reader.getStripes()).as("Should create only one stripe").hasSize(1);
    }

    orcFile.deleteOnExit();

    return appender;
  }

  @Override
  protected boolean supportsVariant() {
    return false;
  }

  @Test
  public void testIsNaN() {
    boolean shouldRead = shouldRead(isNaN("all_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(isNaN("some_nans"));
    assertThat(shouldRead)
        .as("Should read: NaN counts are not tracked in Parquet metrics")
        .isTrue();

    shouldRead = shouldRead(isNaN("no_nans"));
    assertThat(shouldRead).isFalse();

    shouldRead = shouldRead(isNaN("all_nulls"));
    assertThat(shouldRead).as("Should skip: all null column will not contain nan value").isFalse();
  }

  @Test
  public void testIntegerNotIn() {
    super.testIntegerNotIn();
    // no_nulls column has all values == "", so notIn("no_nulls", "") should always be false and
    // so should be skipped
    boolean shouldRead = shouldRead(notIn("no_nulls", "aaa", ""));
    assertThat(shouldRead).as("Should not read: notIn on no nulls column").isFalse();
  }

  @Override
  protected boolean shouldRead(Expression expression, boolean caseSensitive) {
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(orcFile))
            .project(schema())
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema(), fileSchema))
            .filter(expression)
            .caseSensitive(caseSensitive)
            .build()) {
      return !Lists.newArrayList(reader).isEmpty();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
