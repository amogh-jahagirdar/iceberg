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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestPartitioningWriters<T> extends WriterTestBase<T> {

  @Parameters(name = "formatVersion = {0}, fileFormat = {1}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {2, FileFormat.AVRO},
        new Object[] {2, FileFormat.PARQUET},
        new Object[] {2, FileFormat.ORC});
  }

  private static final long TARGET_FILE_SIZE = 128L * 1024 * 1024;

  @Parameter(index = 1)
  private FileFormat fileFormat;

  private OutputFileFactory fileFactory = null;

  protected abstract StructLikeSet toSet(Iterable<T> records);

  protected FileFormat format() {
    return fileFormat;
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  @TestTemplate
  public void testClusteredDataWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    ClusteredDataWriter<T> writer =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    writer.close();
    assertThat(writer.result().dataFiles()).isEmpty();

    writer.close();
    assertThat(writer.result().dataFiles()).isEmpty();
  }

  @TestTemplate
  public void testClusteredDataWriterMultiplePartitions() throws IOException {
    table.updateSpec().addField(Expressions.ref("data")).commit();

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    ClusteredDataWriter<T> writer =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec spec = table.spec();

    writer.write(toRow(1, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(2, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(3, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(4, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(5, "ccc"), spec, partitionKey(spec, "ccc"));

    writer.close();

    DataWriteResult result = writer.result();
    assertThat(result.dataFiles()).hasSize(3);

    RowDelta rowDelta = table.newRowDelta();
    result.dataFiles().forEach(rowDelta::addRows);
    rowDelta.commit();

    List<T> expectedRows =
        ImmutableList.of(
            toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "bbb"), toRow(4, "bbb"), toRow(5, "ccc"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testClusteredDataWriterOutOfOrderPartitions() throws IOException {
    table.updateSpec().addField(Expressions.ref("data")).commit();

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    ClusteredDataWriter<T> writer =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec spec = table.spec();

    writer.write(toRow(1, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(2, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(3, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(4, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(5, "ccc"), spec, partitionKey(spec, "ccc"));

    assertThatThrownBy(() -> writer.write(toRow(6, "aaa"), spec, partitionKey(spec, "aaa")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Encountered records that belong to already closed files")
        .hasMessageEndingWith("partition 'data=aaa' in spec " + spec);

    writer.close();
  }

  @TestTemplate
  public void testClusteredEqualityDeleteWriterNoRecords() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);
    ClusteredEqualityDeleteWriter<T> writer =
        new ClusteredEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();
  }

  @TestTemplate
  public void testClusteredEqualityDeleteWriterMultipleSpecs() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);

    // add an unpartitioned data file
    ImmutableList<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // partition by bucket
    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    // add a data file partitioned by bucket
    ImmutableList<T> rows2 = ImmutableList.of(toRow(3, "bbb"), toRow(4, "bbb"), toRow(12, "bbb"));
    DataFile dataFile2 =
        writeData(
            writerFactory, fileFactory, rows2, table.spec(), partitionKey(table.spec(), "bbb"));
    table.newFastAppend().appendFile(dataFile2).commit();

    // partition by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file partitioned by data
    ImmutableList<T> rows3 = ImmutableList.of(toRow(5, "ccc"), toRow(13, "ccc"));
    DataFile dataFile3 =
        writeData(
            writerFactory, fileFactory, rows3, table.spec(), partitionKey(table.spec(), "ccc"));
    table.newFastAppend().appendFile(dataFile3).commit();

    ClusteredEqualityDeleteWriter<T> writer =
        new ClusteredEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(toRow(1, "aaa"), unpartitionedSpec, null);
    writer.write(toRow(2, "aaa"), unpartitionedSpec, null);
    writer.write(toRow(3, "bbb"), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(toRow(4, "bbb"), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(toRow(5, "ccc"), identitySpec, partitionKey(identitySpec, "ccc"));

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(3);
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();

    RowDelta rowDelta = table.newRowDelta();
    result.deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    List<T> expectedRows = ImmutableList.of(toRow(11, "aaa"), toRow(12, "bbb"), toRow(13, "ccc"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testClusteredEqualityDeleteWriterOutOfOrderSpecsAndPartitions() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    ClusteredEqualityDeleteWriter<T> writer =
        new ClusteredEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(toRow(1, "aaa"), unpartitionedSpec, null);
    writer.write(toRow(2, "aaa"), unpartitionedSpec, null);
    writer.write(toRow(3, "bbb"), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(toRow(4, "bbb"), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(toRow(5, "ccc"), identitySpec, partitionKey(identitySpec, "ccc"));
    writer.write(toRow(6, "ddd"), identitySpec, partitionKey(identitySpec, "ddd"));

    assertThatThrownBy(
            () -> writer.write(toRow(7, "ccc"), identitySpec, partitionKey(identitySpec, "ccc")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Encountered records that belong to already closed files")
        .hasMessageEndingWith("partition 'data=ccc' in spec " + identitySpec);

    assertThatThrownBy(() -> writer.write(toRow(7, "aaa"), unpartitionedSpec, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Encountered records that belong to already closed files")
        .hasMessageEndingWith("spec []");

    writer.close();
  }

  @TestTemplate
  public void testClusteredPositionDeleteWriterNoRecordsPartitionGranularity() throws IOException {
    checkClusteredPositionDeleteWriterNoRecords(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testClusteredPositionDeleteWriterNoRecordsFileGranularity() throws IOException {
    checkClusteredPositionDeleteWriterNoRecords(DeleteGranularity.FILE);
  }

  private void checkClusteredPositionDeleteWriterNoRecords(DeleteGranularity deleteGranularity)
      throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    ClusteredPositionDeleteWriter<T> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE, deleteGranularity);

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();
  }

  @TestTemplate
  public void testClusteredPositionDeleteWriterMultipleSpecsPartitionGranularity()
      throws IOException {
    checkClusteredPositionDeleteWriterMultipleSpecs(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testClusteredPositionDeleteWriterMultipleSpecsFileGranularity() throws IOException {
    checkClusteredPositionDeleteWriterMultipleSpecs(DeleteGranularity.FILE);
  }

  private void checkClusteredPositionDeleteWriterMultipleSpecs(DeleteGranularity deleteGranularity)
      throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add an unpartitioned data file
    ImmutableList<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // partition by bucket
    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    // add a data file partitioned by bucket
    ImmutableList<T> rows2 = ImmutableList.of(toRow(3, "bbb"), toRow(4, "bbb"), toRow(12, "bbb"));
    DataFile dataFile2 =
        writeData(
            writerFactory, fileFactory, rows2, table.spec(), partitionKey(table.spec(), "bbb"));
    table.newFastAppend().appendFile(dataFile2).commit();

    // partition by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file partitioned by data
    ImmutableList<T> rows3 = ImmutableList.of(toRow(5, "ccc"), toRow(13, "ccc"));
    DataFile dataFile3 =
        writeData(
            writerFactory, fileFactory, rows3, table.spec(), partitionKey(table.spec(), "ccc"));
    table.newFastAppend().appendFile(dataFile3).commit();

    ClusteredPositionDeleteWriter<T> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE, deleteGranularity);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(positionDelete(dataFile1.location(), 0L, null), unpartitionedSpec, null);
    writer.write(positionDelete(dataFile1.location(), 1L, null), unpartitionedSpec, null);
    writer.write(
        positionDelete(dataFile2.location(), 0L, null),
        bucketSpec,
        partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete(dataFile2.location(), 1L, null),
        bucketSpec,
        partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete(dataFile3.location(), 0L, null),
        identitySpec,
        partitionKey(identitySpec, "ccc"));

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(writer.result().deleteFiles()).hasSize(3);
    assertThat(writer.result().referencedDataFiles()).hasSize(3);
    assertThat(writer.result().referencesDataFiles()).isTrue();

    RowDelta rowDelta = table.newRowDelta();
    result.deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    List<T> expectedRows = ImmutableList.of(toRow(11, "aaa"), toRow(12, "bbb"), toRow(13, "ccc"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testClusteredPositionDeleteWriterOutOfOrderSpecsAndPartitionsPartitionGranularity()
      throws IOException {
    checkClusteredPositionDeleteWriterOutOfOrderSpecsAndPartitions(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testClusteredPositionDeleteWriterOutOfOrderSpecsAndPartitionsFileGranularity()
      throws IOException {
    checkClusteredPositionDeleteWriterOutOfOrderSpecsAndPartitions(DeleteGranularity.FILE);
  }

  private void checkClusteredPositionDeleteWriterOutOfOrderSpecsAndPartitions(
      DeleteGranularity deleteGranularity) throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    ClusteredPositionDeleteWriter<T> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE, deleteGranularity);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(positionDelete("file-1.parquet", 0L, null), unpartitionedSpec, null);
    writer.write(positionDelete("file-1.parquet", 1L, null), unpartitionedSpec, null);
    writer.write(
        positionDelete("file-2.parquet", 0L, null), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete("file-2.parquet", 1L, null), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete("file-3.parquet", 0L, null),
        identitySpec,
        partitionKey(identitySpec, "ccc"));
    writer.write(
        positionDelete("file-4.parquet", 0L, null),
        identitySpec,
        partitionKey(identitySpec, "ddd"));

    assertThatThrownBy(
            () -> {
              PositionDelete<T> positionDelete = positionDelete("file-5.parquet", 1L, null);
              writer.write(positionDelete, identitySpec, partitionKey(identitySpec, "ccc"));
            })
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Encountered records that belong to already closed files")
        .hasMessageEndingWith("partition 'data=ccc' in spec " + identitySpec);

    assertThatThrownBy(
            () -> {
              PositionDelete<T> positionDelete = positionDelete("file-1.parquet", 3L, null);
              writer.write(positionDelete, unpartitionedSpec, null);
            })
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Encountered records that belong to already closed files")
        .hasMessageEndingWith("spec []");

    writer.close();
  }

  @TestTemplate
  public void testClusteredPositionDeleteWriterPartitionGranularity() throws IOException {
    checkClusteredPositionDeleteWriterGranularity(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testClusteredPositionDeleteWriterFileGranularity() throws IOException {
    checkClusteredPositionDeleteWriterGranularity(DeleteGranularity.FILE);
  }

  private void checkClusteredPositionDeleteWriterGranularity(DeleteGranularity deleteGranularity)
      throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add the first data file
    List<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // add the second data file
    List<T> rows2 = ImmutableList.of(toRow(3, "aaa"), toRow(4, "aaa"), toRow(12, "aaa"));
    DataFile dataFile2 = writeData(writerFactory, fileFactory, rows2, table.spec(), null);
    table.newFastAppend().appendFile(dataFile2).commit();

    // init the delete writer
    ClusteredPositionDeleteWriter<T> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE, deleteGranularity);

    PartitionSpec spec = table.spec();

    // write deletes for both data files
    writer.write(positionDelete(dataFile1.location(), 0L, null), spec, null);
    writer.write(positionDelete(dataFile1.location(), 1L, null), spec, null);
    writer.write(positionDelete(dataFile2.location(), 0L, null), spec, null);
    writer.write(positionDelete(dataFile2.location(), 1L, null), spec, null);
    writer.close();

    // verify the writer result
    DeleteWriteResult result = writer.result();
    int expectedNumDeleteFiles = deleteGranularity == DeleteGranularity.FILE ? 2 : 1;
    assertThat(result.deleteFiles()).hasSize(expectedNumDeleteFiles);
    assertThat(result.referencedDataFiles()).hasSize(2);
    assertThat(result.referencesDataFiles()).isTrue();

    // commit the deletes
    RowDelta rowDelta = table.newRowDelta();
    result.deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    // verify correctness
    List<T> expectedRows = ImmutableList.of(toRow(11, "aaa"), toRow(12, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testFanoutDataWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    FanoutDataWriter<T> writer =
        new FanoutDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    writer.close();
    assertThat(writer.result().dataFiles()).isEmpty();

    writer.close();
    assertThat(writer.result().dataFiles()).isEmpty();
  }

  @TestTemplate
  public void testFanoutDataWriterMultiplePartitions() throws IOException {
    table.updateSpec().addField(Expressions.ref("data")).commit();

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    FanoutDataWriter<T> writer =
        new FanoutDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec spec = table.spec();

    writer.write(toRow(1, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(3, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(2, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(4, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(5, "ccc"), spec, partitionKey(spec, "ccc"));

    writer.close();

    DataWriteResult result = writer.result();
    assertThat(result.dataFiles()).hasSize(3);

    RowDelta rowDelta = table.newRowDelta();
    result.dataFiles().forEach(rowDelta::addRows);
    rowDelta.commit();

    List<T> expectedRows =
        ImmutableList.of(
            toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "bbb"), toRow(4, "bbb"), toRow(5, "ccc"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testFanoutPositionOnlyDeleteWriterNoRecordsPartitionGranularity() throws IOException {
    checkFanoutPositionOnlyDeleteWriterNoRecords(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testFanoutPositionOnlyDeleteWriterNoRecordsFileGranularity() throws IOException {
    checkFanoutPositionOnlyDeleteWriterNoRecords(DeleteGranularity.FILE);
  }

  private void checkFanoutPositionOnlyDeleteWriterNoRecords(DeleteGranularity deleteGranularity)
      throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    FanoutPositionOnlyDeleteWriter<T> writer =
        new FanoutPositionOnlyDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE, deleteGranularity);

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();
  }

  @TestTemplate
  public void testFanoutPositionOnlyDeleteWriterOutOfOrderRecordsPartitionGranularity()
      throws IOException {
    checkFanoutPositionOnlyDeleteWriterOutOfOrderRecords(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testFanoutPositionOnlyDeleteWriterOutOfOrderRecordsFileGranularity()
      throws IOException {
    checkFanoutPositionOnlyDeleteWriterOutOfOrderRecords(DeleteGranularity.FILE);
  }

  private void checkFanoutPositionOnlyDeleteWriterOutOfOrderRecords(
      DeleteGranularity deleteGranularity) throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add an unpartitioned data file
    ImmutableList<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // partition by bucket
    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    // add a data file partitioned by bucket
    ImmutableList<T> rows2 = ImmutableList.of(toRow(3, "bbb"), toRow(4, "bbb"), toRow(12, "bbb"));
    DataFile dataFile2 =
        writeData(
            writerFactory, fileFactory, rows2, table.spec(), partitionKey(table.spec(), "bbb"));
    table.newFastAppend().appendFile(dataFile2).commit();

    // partition by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file partitioned by data
    ImmutableList<T> rows3 = ImmutableList.of(toRow(5, "ccc"), toRow(13, "ccc"));
    DataFile dataFile3 =
        writeData(
            writerFactory, fileFactory, rows3, table.spec(), partitionKey(table.spec(), "ccc"));
    table.newFastAppend().appendFile(dataFile3).commit();

    FanoutPositionOnlyDeleteWriter<T> writer =
        new FanoutPositionOnlyDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE, deleteGranularity);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(positionDelete(dataFile1.location(), 1L, null), unpartitionedSpec, null);
    writer.write(
        positionDelete(dataFile2.location(), 1L, null),
        bucketSpec,
        partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete(dataFile2.location(), 0L, null),
        bucketSpec,
        partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete(dataFile3.location(), 1L, null),
        identitySpec,
        partitionKey(identitySpec, "ccc"));
    writer.write(
        positionDelete(dataFile3.location(), 2L, null),
        identitySpec,
        partitionKey(identitySpec, "ccc"));
    writer.write(positionDelete(dataFile1.location(), 0L, null), unpartitionedSpec, null);
    writer.write(
        positionDelete(dataFile3.location(), 0L, null),
        identitySpec,
        partitionKey(identitySpec, "ccc"));
    writer.write(positionDelete(dataFile1.location(), 2L, null), unpartitionedSpec, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(writer.result().deleteFiles()).hasSize(3);
    assertThat(writer.result().referencedDataFiles()).hasSize(3);
    assertThat(writer.result().referencesDataFiles()).isTrue();

    RowDelta rowDelta = table.newRowDelta();
    result.deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    List<T> expectedRows = ImmutableList.of(toRow(12, "bbb"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testFanoutPositionOnlyDeleteWriterPartitionGranularity() throws IOException {
    checkFanoutPositionOnlyDeleteWriterGranularity(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testFanoutPositionOnlyDeleteWriterFileGranularity() throws IOException {
    checkFanoutPositionOnlyDeleteWriterGranularity(DeleteGranularity.FILE);
  }

  private void checkFanoutPositionOnlyDeleteWriterGranularity(DeleteGranularity deleteGranularity)
      throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add the first data file
    List<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // add the second data file
    List<T> rows2 = ImmutableList.of(toRow(3, "aaa"), toRow(4, "aaa"), toRow(12, "aaa"));
    DataFile dataFile2 = writeData(writerFactory, fileFactory, rows2, table.spec(), null);
    table.newFastAppend().appendFile(dataFile2).commit();

    // init the delete writer
    FanoutPositionOnlyDeleteWriter<T> writer =
        new FanoutPositionOnlyDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE, deleteGranularity);

    PartitionSpec spec = table.spec();

    // write deletes for both data files (the order of records is mixed)
    writer.write(positionDelete(dataFile1.location(), 1L, null), spec, null);
    writer.write(positionDelete(dataFile2.location(), 0L, null), spec, null);
    writer.write(positionDelete(dataFile1.location(), 0L, null), spec, null);
    writer.write(positionDelete(dataFile2.location(), 1L, null), spec, null);
    writer.close();

    // verify the writer result
    DeleteWriteResult result = writer.result();
    int expectedNumDeleteFiles = deleteGranularity == DeleteGranularity.FILE ? 2 : 1;
    assertThat(result.deleteFiles()).hasSize(expectedNumDeleteFiles);
    assertThat(result.referencedDataFiles()).hasSize(2);
    assertThat(result.referencesDataFiles()).isTrue();

    // commit the deletes
    RowDelta rowDelta = table.newRowDelta();
    result.deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    // verify correctness
    List<T> expectedRows = ImmutableList.of(toRow(11, "aaa"), toRow(12, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testRewriteOfPreviousDeletes() throws IOException {
    assumeThat(format()).isIn(FileFormat.PARQUET, FileFormat.ORC);

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add the first data file
    List<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // add the second data file
    List<T> rows2 = ImmutableList.of(toRow(3, "aaa"), toRow(4, "aaa"), toRow(12, "aaa"));
    DataFile dataFile2 = writeData(writerFactory, fileFactory, rows2, table.spec(), null);
    table.newFastAppend().appendFile(dataFile2).commit();

    PartitionSpec spec = table.spec();

    // init the first delete writer without access to previous deletes
    FanoutPositionOnlyDeleteWriter<T> writer1 =
        new FanoutPositionOnlyDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE, DeleteGranularity.FILE);

    // write initial deletes for both data files
    writer1.write(positionDelete(dataFile1.location(), 1L), spec, null);
    writer1.write(positionDelete(dataFile2.location(), 1L), spec, null);
    writer1.close();

    // verify the writer result
    DeleteWriteResult result1 = writer1.result();
    assertThat(result1.deleteFiles()).hasSize(2);
    assertThat(result1.referencedDataFiles()).hasSize(2);
    assertThat(result1.referencesDataFiles()).isTrue();
    assertThat(result1.rewrittenDeleteFiles()).isEmpty();

    // commit the initial deletes
    RowDelta rowDelta1 = table.newRowDelta();
    result1.deleteFiles().forEach(rowDelta1::addDeletes);
    rowDelta1.commit();

    // verify correctness of the first delete operation
    List<T> expectedRows1 =
        ImmutableList.of(toRow(1, "aaa"), toRow(3, "aaa"), toRow(11, "aaa"), toRow(12, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows1));

    // populate previous delete mapping
    Map<String, DeleteFile> previousDeletes = Maps.newHashMap();
    for (DeleteFile deleteFile : result1.deleteFiles()) {
      String dataLocation = ContentFileUtil.referencedDataFile(deleteFile).toString();
      previousDeletes.put(dataLocation, deleteFile);
    }

    // init the second delete writer with access to previous deletes
    FanoutPositionOnlyDeleteWriter<T> writer2 =
        new FanoutPositionOnlyDeleteWriter<>(
            writerFactory,
            fileFactory,
            table.io(),
            TARGET_FILE_SIZE,
            DeleteGranularity.FILE,
            new PreviousDeleteLoader(table, previousDeletes));

    // write more deletes for both data files
    writer2.write(positionDelete(dataFile1.location(), 0L), spec, null);
    writer2.write(positionDelete(dataFile2.location(), 0L), spec, null);
    writer2.close();

    // verify the writer result
    DeleteWriteResult result2 = writer2.result();
    assertThat(result2.deleteFiles()).hasSize(2);
    assertThat(result2.referencedDataFiles()).hasSize(2);
    assertThat(result2.referencesDataFiles()).isTrue();
    assertThat(result2.rewrittenDeleteFiles()).hasSize(2);

    // add new and remove rewritten delete files
    RowDelta rowDelta2 = table.newRowDelta();
    result2.deleteFiles().forEach(rowDelta2::addDeletes);
    result2.rewrittenDeleteFiles().forEach(rowDelta2::removeDeletes);
    rowDelta2.commit();

    // verify correctness of the second delete operation
    List<T> expectedRows2 = ImmutableList.of(toRow(11, "aaa"), toRow(12, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows2));
  }

  private static class PreviousDeleteLoader implements Function<CharSequence, PositionDeleteIndex> {
    private final Map<String, DeleteFile> deleteFiles;
    private final DeleteLoader deleteLoader;

    PreviousDeleteLoader(Table table, Map<String, DeleteFile> deleteFiles) {
      this.deleteFiles = deleteFiles;
      this.deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
    }

    @Override
    public PositionDeleteIndex apply(CharSequence path) {
      DeleteFile deleteFile = deleteFiles.get(path);
      return deleteLoader.loadPositionDeletes(ImmutableList.of(deleteFile), path);
    }
  }
}
