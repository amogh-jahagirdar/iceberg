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
import java.util.ArrayDeque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.ThreadPools;

public class RESTTableScan extends DataTableScan {
  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final TableOperations operations;
  private final Table table;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final ParserContext parserContext;

  // TODO revisit if this property should be configurable
  // Sleep duration between polling attempts for plan completion
  private static final int FETCH_PLANNING_SLEEP_DURATION_MS = 1000;
  // Maximum time to wait for scan planning to complete (5 minutes)
  // This prevents indefinite blocking on server-side planning issues
  private static final long MAX_WAIT_TIME_MS = 5 * 60 * 1000L;

  RESTTableScan(
      Table table,
      Schema schema,
      TableScanContext context,
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      TableOperations operations,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths) {
    super(table, schema, context);
    this.table = table;
    this.client = client;
    this.headers = headers;
    this.path = path;
    this.operations = operations;
    this.tableIdentifier = tableIdentifier;
    this.resourcePaths = resourcePaths;
    this.parserContext =
        ParserContext.builder()
            .add("specsById", table.specs())
            .add("caseSensitive", context().caseSensitive())
            .build();
  }

  @Override
  protected TableScan newRefinedScan(
      Table refinedTable, Schema refinedSchema, TableScanContext refinedContext) {
    return new RESTTableScan(
        refinedTable,
        refinedSchema,
        refinedContext,
        client,
        path,
        headers,
        operations,
        tableIdentifier,
        resourcePaths);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Long startSnapshotId = context().fromSnapshotId();
    Long endSnapshotId = context().toSnapshotId();
    Long snapshotId = snapshotId();
    List<String> selectedColumns =
        schema().columns().stream().map(Types.NestedField::name).collect(Collectors.toList());

    List<String> statsFields = null;
    if (columnsToKeepStats() != null) {
      statsFields =
          columnsToKeepStats().stream()
              .map(columnId -> schema().findColumnName(columnId))
              .collect(Collectors.toList());
    }

    PlanTableScanRequest.Builder planTableScanRequestBuilder =
        new PlanTableScanRequest.Builder()
            .withSelect(selectedColumns)
            .withFilter(filter())
            .withCaseSensitive(isCaseSensitive())
            .withStatsFields(statsFields);

    if (startSnapshotId != null && endSnapshotId != null) {
      planTableScanRequestBuilder
          .withStartSnapshotId(startSnapshotId)
          .withEndSnapshotId(endSnapshotId)
          .withUseSnapshotSchema(true);

    } else if (snapshotId != null) {
      boolean useSnapShotSchema = snapshotId != table.currentSnapshot().snapshotId();
      planTableScanRequestBuilder
          .withSnapshotId(snapshotId)
          .withUseSnapshotSchema(useSnapShotSchema);

    } else {
      planTableScanRequestBuilder.withSnapshotId(table().currentSnapshot().snapshotId());
    }

    return planTableScan(planTableScanRequestBuilder.build());
  }

  private CloseableIterable<FileScanTask> planTableScan(PlanTableScanRequest planTableScanRequest) {

    PlanTableScanResponse response =
        client.post(
            resourcePaths.planTableScan(tableIdentifier),
            planTableScanRequest,
            PlanTableScanResponse.class,
            headers.get(),
            ErrorHandlers.defaultErrorHandler(),
            stringStringMap -> {},
            parserContext);

    PlanStatus planStatus = response.planStatus();
    switch (planStatus) {
      case COMPLETED:
        return new FlatteningTaskIterable(response.fileScanTasks(), response.planTasks());
      case SUBMITTED:
        return new FlatteningTaskIterable(Lists.newArrayList(), ImmutableList.of(response.planId()));
      case FAILED:
        throw new IllegalStateException(
            "Received \"failed\" status from service when planning a table scan");
      case CANCELLED:
        throw new IllegalStateException(
            "Received \"cancelled\" status from service when planning a table scan");
      default:
        throw new RuntimeException(
            String.format("Invalid planStatus during planTableScan: %s", planStatus));
    }
  }

  static class FetchPlanTask implements Runnable {
      private final Queue<FileScanTask> results;
      private final Queue<String> childTasks;

      FetchPlanTask(String planTask, Queue<FileScanTask> results, Queue<String> childPlanTasks) {
          this.results = results;
          this.childTasks = childPlanTasks;
      }

      @Override
      public void run() {
          return "";
      }
  }

  static class FlatteningTaskIterable implements CloseableIterable<FileScanTask> {
      private final Queue<FileScanTask> results;
      private final Queue<String> planTasksToFetch;
      private final ExecutorService planTaskWorkers = ThreadPools.getWorkerPool();

      FlatteningTaskIterable(List<FileScanTask> tasks, List<String> planTasks) {
          this.results = new LinkedBlockingQueue<>(Math.max(tasks.size(), 28_000));
          for (FileScanTask task: tasks) {
              results.offer(task);
          }

          this.planTasksToFetch = new LinkedBlockingQueue<>(Math.max(planTasks.size(), 10));
          for (String planTask: planTasks) {
              planTasksToFetch.offer(planTask);
          }

          fetchPlanTasks();
      }

      @Override
      public CloseableIterator<FileScanTask> iterator() {
          return CloseableIterator.withClose(results.iterator());
      }

      @Override
      public void close() throws IOException {
          // call close on all the planTasksToFetch
      }

      private void fetchPlanTasks() {
          for (int i = 0; i < ThreadPools.WORKER_THREAD_POOL_SIZE; i++) {
              planTaskWorkers.submit(new Runnable() {
                  @Override
                  public void run() {
                      try {
                          while(true) {
                              String planTask = planTasksToFetch.poll();
                              // Nothing on the queue
                              if (planTask == null) {
                                  break;
                              }

                              CloseableIterable<FileScanTask> tasks = fetchPl
                          }
                      } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                      }
                  }
              });
          }
      }
  }

  @VisibleForTesting
  @SuppressWarnings("checkstyle:RegexpMultiline")
  public boolean cancelPlan() {
      // Go through any open work items and cancel those
      return true;
  }
}
