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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ParallelIterable;

/** Plans file scan tasks from a v4 root manifest and the leaf manifests that it references. */
class ManifestExpander {
  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

  private final FileIO io;
  private final ManifestFile rootManifest;
  private final Schema tableSchema;
  private final Map<Integer, PartitionSpec> specsById;
  private final String tableLocation;
  private Expression dataFilter = Expressions.alwaysTrue();
  private boolean ignoreResiduals = false;
  private boolean caseSensitive = true;
  private ScanMetrics scanMetrics = ScanMetrics.noop();
  private ExecutorService executorService = null;

  ManifestExpander(
      FileIO io,
      ManifestFile rootManifest,
      Schema tableSchema,
      Map<Integer, PartitionSpec> specsById,
      String tableLocation) {
    this.io = io;
    this.rootManifest = rootManifest;
    this.tableSchema = tableSchema;
    this.specsById = specsById;
    this.tableLocation = tableLocation;
  }

  ManifestExpander filterData(Expression newDataFilter) {
    this.dataFilter = Expressions.and(dataFilter, newDataFilter);
    return this;
  }

  ManifestExpander ignoreResiduals() {
    this.ignoreResiduals = true;
    return this;
  }

  ManifestExpander caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  ManifestExpander scanMetrics(ScanMetrics newScanMetrics) {
    this.scanMetrics = newScanMetrics;
    return this;
  }

  ManifestExpander planWith(ExecutorService newExecutorService) {
    this.executorService = newExecutorService;
    return this;
  }

  CloseableIterable<FileScanTask> planFiles() {
    Map<Integer, TaskContext> contexts = Maps.newHashMap();
    Expression residualFilter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
    for (PartitionSpec spec : specsById.values()) {
      contexts.put(spec.specId(), new TaskContext(spec, residualFilter, caseSensitive));
    }

    List<TrackedFile> dataFiles = Lists.newArrayList();
    List<CloseableIterable<FileScanTask>> tasks = Lists.newArrayList();
    try (V4ManifestReader reader = newReader(rootManifest)) {
      for (TrackedFile file : reader) {
        switch (file.contentType()) {
          case DATA -> dataFiles.add(file);
          case DATA_MANIFEST ->
              tasks.add(
                  CloseableIterable.transform(
                      newReader(TrackedFileAdapters.asManifestFile(file)),
                      dataFile -> toTask(dataFile, contexts)));
          default -> {
            // delete manifests are not applied
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read root manifest: %s", rootManifest.path());
    }

    if (!dataFiles.isEmpty()) {
      tasks.add(
          CloseableIterable.transform(
              CloseableIterable.withNoopClose(dataFiles), dataFile -> toTask(dataFile, contexts)));
    }

    if (executorService != null) {
      return new ParallelIterable<>(tasks, executorService);
    }

    return CloseableIterable.concat(tasks);
  }

  private V4ManifestReader newReader(ManifestFile manifest) {
    return V4ManifestReader.builder(manifest, io, tableSchema, specsById)
        .tableLocation(tableLocation)
        .filter(dataFilter)
        .caseSensitive(caseSensitive)
        .scanMetrics(scanMetrics)
        .forScanPlanning()
        .build();
  }

  private FileScanTask toTask(TrackedFile file, Map<Integer, TaskContext> contexts) {
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, specsById);
    DeleteFile[] deletes =
        file.deletionVector() != null
            ? new DeleteFile[] {TrackedFileAdapters.asDVDeleteFile(file, specsById)}
            : NO_DELETES;
    ScanMetricsUtil.fileTask(scanMetrics, dataFile, deletes);

    TaskContext context = contexts.get(dataFile.specId());
    return new BaseFileScanTask(
        dataFile, deletes, context.schemaString, context.specString, context.residuals);
  }

  private static class TaskContext {
    private final String schemaString;
    private final String specString;
    private final ResidualEvaluator residuals;

    private TaskContext(PartitionSpec spec, Expression filter, boolean caseSensitive) {
      this.schemaString = SchemaParser.toJson(spec.schema());
      this.specString = PartitionSpecParser.toJson(spec);
      this.residuals = ResidualEvaluator.of(spec, filter, caseSensitive);
    }
  }
}
