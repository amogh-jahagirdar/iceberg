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
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
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
        return new FlatteningTaskIterable(ImmutableList.of(), ImmutableList.of(response.planId()));
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

  class FetchPlanTask {
    private final Queue<FileScanTask> results;
    private final Queue<String> childTasks;

    FetchPlanTask(Queue<FileScanTask> results, Queue<String> childPlanTasks) {
      this.results = results;
      this.childTasks = childPlanTasks;
    }

    public void processPlanTask(String planTask) {
      try {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= MAX_WAIT_TIME_MS) {
          FetchPlanningResultResponse response =
              client.get(
                  resourcePaths.plan(tableIdentifier, planTask),
                  Map.of(),
                  FetchPlanningResultResponse.class,
                  headers.get(),
                  ErrorHandlers.defaultErrorHandler(),
                  parserContext);

          PlanStatus planStatus = response.planStatus();
          switch (planStatus) {
            case COMPLETED:
              response.fileScanTasks().forEach(results::offer);
              response.planTasks().forEach(childTasks::offer);
              return;
            case SUBMITTED:
              try {
                Thread.sleep(FETCH_PLANNING_SLEEP_DURATION_MS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Attempt to cancel the plan before exiting
                cancelPlan(planTask);
                throw new RuntimeException("Interrupted while fetching plan status", e);
              }
              break;
            case FAILED:
              throw new IllegalStateException(
                  "Received \"failed\" status from service when fetching a table scan");
            case CANCELLED:
              throw new IllegalStateException(
                  String.format(
                      Locale.ROOT,
                      "Received \"cancelled\" status from service when fetching a table scan, planId: %s is invalid",
                      planTask));
            default:
              throw new IllegalStateException(
                  String.format(
                      Locale.ROOT,
                      "Invalid planStatus during fetchPlanningResult: %s",
                      planStatus));
          }
        }
        // If we reach here, we've exceeded the max wait time
        // Attempt to cancel the plan before timing out
        cancelPlan(planTask);
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Exceeded max wait time of %d ms when fetching planning result for planId: %s",
                MAX_WAIT_TIME_MS,
                planTask));
      } catch (Exception e) {
        // Clear the plan ID on any exception (except successful completion)
        // Also attempt to cancel the plan to clean up server resources
        try {
          cancelPlan(planTask);
        } catch (Exception cancelException) {
          // Ignore cancellation failures during exception handling
          // The original exception is more important
        }
        throw e;
      }
    }

    void cancelPlan(String planTask) {
      try {
        client.delete(
            resourcePaths.plan(tableIdentifier, planTask),
            Map.of(),
            null,
            headers.get(),
            ErrorHandlers.defaultErrorHandler());
      } catch (Exception e) {
        // Plan might have already completed or failed, which is acceptableWW
      }
    }
  }

  class FlatteningTaskIterable implements CloseableIterable<FileScanTask> {
    private final Queue<FileScanTask> results;
    private final ExecutorService planTaskWorkers = ThreadPools.getWorkerPool();
    private Queue<String> planTasksToFetch;

    FlatteningTaskIterable(List<FileScanTask> tasks, List<String> planTasks) {
      this.results = new LinkedBlockingQueue<>(Math.max(tasks.size(), 28_000));
      for (FileScanTask task : tasks) {
        results.offer(task);
      }

      if (planTasks != null) {
          this.planTasksToFetch = new LinkedBlockingQueue<>(Math.max(planTasks.size(), 10));
          startFetchingPlanTasks();
      }
    }

    @Override
    public CloseableIterator<FileScanTask> iterator() {
      return CloseableIterator.withClose(results.iterator());
    }

    void cancelPlan(String planTask) {
      try {
        client.delete(
            resourcePaths.plan(tableIdentifier, planTask),
            Map.of(),
            null,
            headers.get(),
            ErrorHandlers.defaultErrorHandler());
      } catch (Exception e) {
        // Plan might have already completed or failed, which is acceptableWW
      }
    }

    @Override
    public void close() throws IOException {
      // call close on all the planTasksToFetch
      planTaskWorkers.shutdown();

      // Cancel ongoing plan tasks
      for (String planTask : planTasksToFetch) {
        cancelPlan(planTask);
      }
    }

    void startFetchingPlanTasks() {
      FetchPlanTask fetchPlanTask = new FetchPlanTask(results, planTasksToFetch);
      for (int i = 0; i < ThreadPools.WORKER_THREAD_POOL_SIZE; i++) {
        planTaskWorkers.execute(
            () -> {
              try {
                while (true) {
                  String planTask = planTasksToFetch.poll();
                  if (planTask == null) {
                    break;
                  }

                  fetchPlanTask.processPlanTask(planTask);
                }
              } catch (Throwable t) {
                // Optionally: handle/log exceptions as appropriate
              }
            });
      }
    }
  }
}
