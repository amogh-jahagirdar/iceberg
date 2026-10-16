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
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestV4ManifestReadWrite {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  private InMemoryCatalog catalog;
  private Table table;

  @BeforeEach
  void before() {
    this.catalog = new InMemoryCatalog();
    catalog.initialize(
        "test", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, "s3://bucket/warehouse"));
    catalog.createNamespace(Namespace.of("db"));
    this.table =
        catalog.createTable(
            TableIdentifier.of("db", "table"),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "4"));
  }

  @AfterEach
  void after() throws IOException {
    catalog.close();
  }

  @Test
  void appendWritesRootManifest() throws IOException {
    DataFile file = dataFile("a.parquet", 10L);
    table.newFastAppend().appendFile(file).commit();

    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.manifestListLocation())
        .startsWith(table.location() + "/metadata/")
        .endsWith(".parquet");
    assertThat(snapshot.firstRowId()).isEqualTo(0L);
    assertThat(snapshot.addedRows()).isEqualTo(10L);

    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(1);
    assertThat(manifests.get(0).formatVersion()).isEqualTo(4);
    assertThat(manifests.get(0).snapshotId()).isEqualTo(snapshot.snapshotId());

    assertThat(firstRowIdsByLocation()).isEqualTo(ImmutableMap.of(file.location(), 0L));
  }

  @Test
  void appendKeepsParentManifests() throws IOException {
    DataFile fileA = dataFile("a.parquet", 10L);
    DataFile fileB = dataFile("b.parquet", 5L);
    table.newFastAppend().appendFile(fileA).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(fileB).commit();

    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.firstRowId()).isEqualTo(10L);
    assertThat(snapshot.allManifests(table.io()))
        .extracting(ManifestFile::snapshotId)
        .containsExactlyInAnyOrder(snapshot.snapshotId(), firstSnapshotId);

    assertThat(firstRowIdsByLocation())
        .isEqualTo(ImmutableMap.of(fileA.location(), 0L, fileB.location(), 10L));
  }

  @Test
  void metadataJsonUsesRelativePaths() throws IOException {
    table.newFastAppend().appendFile(dataFile("a.parquet", 10L)).commit();
    table.newFastAppend().appendFile(dataFile("b.parquet", 5L)).commit();

    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    JsonNode json;
    try (InputStream input = table.io().newInputFile(metadata.metadataFileLocation()).newStream()) {
      json = JsonUtil.mapper().readTree(input);
    }

    assertThat(json.get("snapshots"))
        .hasSize(2)
        .allSatisfy(node -> assertThat(node.get("manifest-list").asText()).startsWith("metadata/"));
    assertThat(json.get("metadata-log"))
        .isNotEmpty()
        .allSatisfy(node -> assertThat(node.get("metadata-file").asText()).startsWith("metadata/"));

    String metadataDir = table.location() + "/metadata/";
    assertThat(metadata.snapshots())
        .allSatisfy(snapshot -> assertThat(snapshot.manifestListLocation()).startsWith(metadataDir));
    assertThat(metadata.previousFiles())
        .allSatisfy(entry -> assertThat(entry.file()).startsWith(metadataDir));
  }

  private DataFile dataFile(String fileName, long recordCount) {
    return DataFiles.builder(table.spec())
        .withPath(table.location() + "/data/" + fileName)
        .withFileSizeInBytes(1024L)
        .withRecordCount(recordCount)
        .build();
  }

  private Map<String, Long> firstRowIdsByLocation() throws IOException {
    Map<String, Long> firstRowIds = Maps.newHashMap();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        firstRowIds.put(task.file().location(), task.file().firstRowId());
      }
    }

    return firstRowIds;
  }
}
