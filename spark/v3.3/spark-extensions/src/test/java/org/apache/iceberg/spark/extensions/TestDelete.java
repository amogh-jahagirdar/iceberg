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
package org.apache.iceberg.spark.extensions;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.SnapshotRef;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestDelete extends TestDeleteBase {

  public TestDelete(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      Boolean vectorized,
      String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS deleted_id");
    sql("DROP TABLE IF EXISTS deleted_dep");
  }

  @Test
  public void testDeleteWithoutScanningTable() throws Exception {
    testDeleteWithoutScanningTable(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteFileThenMetadataDelete() throws Exception {
    testDeleteFileThenMetadataDelete(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithFalseCondition() {
    testDeleteWithFalseCondition(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteFromEmptyTable() {
    testDeleteFromEmptyTable(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testExplain() {
    testExplain(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithAlias() {
    testDeleteWithAlias(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithDynamicFileFiltering() throws NoSuchTableException {
    testDeleteWithDynamicFileFiltering(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteNonExistingRecords() {
    testDeleteNonExistingRecords(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithoutCondition() {
    testDeleteWithoutCondition(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteUsingMetadataWithComplexCondition() {
    testDeleteUsingMetadataWithComplexCondition(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithArbitraryPartitionPredicates() {
    testDeleteWithArbitraryPartitionPredicates(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithNonDeterministicCondition() {
    testDeleteWithNonDeterministicCondition(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithFoldableConditions() {
    testDeleteWithFoldableConditions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithNullConditions() {
    testDeleteWithNullConditions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithInAndNotInConditions() {
    testDeleteWithInAndNotInConditions(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithMultipleRowGroupsParquet() throws NoSuchTableException {
    testDeleteWithMultipleRowGroupsParquet(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithConditionOnNestedColumn() {
    testDeleteWithConditionOnNestedColumn(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithInSubquery() throws NoSuchTableException {
    testDeleteWithInSubquery(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithMultiColumnInSubquery() throws NoSuchTableException {
    testDeleteWithNotInSubquery(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithNotInSubquery() throws NoSuchTableException {
    testDeleteWithNotInSubquery(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteOnNonIcebergTableNotSupported() {
    testDeleteOnNonIcebergTableNotSupported(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithExistSubquery() throws NoSuchTableException {
    testDeleteWithExistSubquery(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithNotExistsSubquery() throws NoSuchTableException {
    testDeleteWithNotExistsSubquery(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithScalarSubquery() throws NoSuchTableException {
    testDeleteWithScalarSubquery(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public synchronized void testDeleteWithSerializableIsolation() throws InterruptedException {
    testDeleteWithSerializableIsolation(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public synchronized void testDeleteWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    testDeleteWithSnapshotIsolation(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteRefreshesRelationCache() throws NoSuchTableException {
    testDeleteRefreshesRelationCache(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteWithMultipleSpecs() {
    testDeleteWithMultipleSpecs(SnapshotRef.MAIN_BRANCH);
  }
}
