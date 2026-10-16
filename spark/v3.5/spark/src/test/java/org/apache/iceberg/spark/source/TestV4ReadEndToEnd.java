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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestV4ReadEndToEnd extends TestBaseWithCatalog {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  @BeforeEach
  void createTable() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '4')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @AfterEach
  void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void readsRows() {
    assertEquals(
        "Should read the inserted rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  void readsFilesTable() {
    assertEquals(
        "Files table should count the inserted rows",
        ImmutableList.of(row(3L)),
        sql("SELECT sum(record_count) FROM %s.files", tableName));
  }

  @TestTemplate
  void writesRootManifestWithRelativeLocation() throws IOException {
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.manifestListLocation())
        .startsWith(table.location() + "/metadata/")
        .endsWith(".parquet");

    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    JsonNode json;
    try (InputStream input = table.io().newInputFile(metadata.metadataFileLocation()).newStream()) {
      json = JsonUtil.mapper().readTree(input);
    }

    String storedLocation = json.get("snapshots").get(0).get("manifest-list").asText();
    assertThat(table.location() + "/" + storedLocation).isEqualTo(snapshot.manifestListLocation());
  }
}
