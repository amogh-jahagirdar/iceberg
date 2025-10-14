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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
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

  private CloseableIterable<FileScanTask> pollSubmittedPlanUntilComplete(String rootPlan) {
    try {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime <= MAX_WAIT_TIME_MS) {
        FetchPlanningResultResponse response =
            client.get(
                resourcePaths.plan(tableIdentifier, rootPlan),
                Map.of(),
                FetchPlanningResultResponse.class,
                headers.get(),
                ErrorHandlers.defaultErrorHandler(),
                parserContext);

        PlanStatus planStatus = response.planStatus();
        switch (planStatus) {
          case COMPLETED:
            return new FlatteningTaskIterable(response.fileScanTasks(), response.planTasks());
          case SUBMITTED:
            try {
              // TODO: if we want to add some jitter here to avoid thundering herd.
              Thread.sleep(FETCH_PLANNING_SLEEP_DURATION_MS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              // Attempt to cancel the plan before exiting
              cancelPlan(rootPlan);
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
                    rootPlan));
          default:
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT, "Invalid planStatus during fetchPlanningResult: %s", planStatus));
        }
      }
      // If we reach here, we've exceeded the max wait time
      // Attempt to cancel the plan before timing out
      cancelPlan(rootPlan);
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Exceeded max wait time of %d ms when fetching planning result for planId: %s",
              MAX_WAIT_TIME_MS,
              rootPlan));
    } catch (Exception e) {
      // Clear the plan ID on any exception (except successful completion)
      // Also attempt to cancel the plan to clean up server resources
      try {
        cancelPlan(rootPlan);
      } catch (Exception cancelException) {
        // Ignore cancellation failures during exception handling
        // The original exception is more important
      }
      throw e;
    }
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
        return new FlatteningTaskIterable(
            response.fileScanTasks() == null ? ImmutableList.of() : response.fileScanTasks(),
            response.planTasks() == null ? ImmutableList.of() : response.planTasks());
      case SUBMITTED:
        return pollSubmittedPlanUntilComplete(response.planId());
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

  class ExpandPlanTask extends RecursiveAction {
    private final Queue<FileScanTask> results;
    private final boolean caseSensitive;
    private final Map<Integer, PartitionSpec> specsById;
    private final String planTaskId;

    ExpandPlanTask(
        String planTask,
        Queue<FileScanTask> results,
        Map<Integer, PartitionSpec> specsById,
        boolean caseSensitive) {
      this.planTaskId = planTask;
      this.results = results;
      this.specsById = specsById;
      this.caseSensitive = caseSensitive;
    }

    @Override
    public void compute() {
      FetchScanTasksRequest fetchScanTasksRequest = new FetchScanTasksRequest(planTaskId);
      ParserContext parserContext =
          ParserContext.builder()
              .add("specsById", specsById)
              .add("caseSensitive", caseSensitive)
              .build();
      FetchScanTasksResponse response =
          client.post(
              resourcePaths.fetchScanTasks(tableIdentifier),
              fetchScanTasksRequest,
              FetchScanTasksResponse.class,
              headers.get(),
              ErrorHandlers.defaultErrorHandler(),
              stringStringMap -> {},
              parserContext);

      if (response.fileScanTasks() != null) {
        response.fileScanTasks().forEach(results::offer);
      }

      if (response.planTasks() != null) {
        List<ExpandPlanTask> children = new ArrayList<>();
        for (String childId : response.planTasks()) {
          children.add(new ExpandPlanTask(childId, results, specsById, caseSensitive));
        }

        if (!children.isEmpty()) {
          invokeAll(children);
        }
      }
    }
  }

  class FlatteningTaskIterable implements CloseableIterable<FileScanTask> {
    private final BlockingQueue<FileScanTask> results;
    private final List<ExpandPlanTask> rootPlanTasks = Lists.newArrayList();
    private final ForkJoinPool forkJoinPool;

    FlatteningTaskIterable(List<FileScanTask> tasks, List<String> planTasks) {
      // ToDo figure out numbers here, give some buffer for plan tasks based on fanout
      // Typically dont expect really any plan tasks anyways
      this.results = new LinkedBlockingQueue<>(Math.max(tasks.size(), 28_000));
      this.forkJoinPool = new ForkJoinPool(ThreadPools.WORKER_THREAD_POOL_SIZE);

      for (FileScanTask task : tasks) {
        results.offer(task);
      }

      if (planTasks != null) {
        for (String planTask : planTasks) {
          rootPlanTasks.add(
              new ExpandPlanTask(planTask, results, table.specs(), context().caseSensitive()));
        }
      }
    }

    @Override
    public CloseableIterator<FileScanTask> iterator() {
      if (!rootPlanTasks.isEmpty()) {
        for (ExpandPlanTask rootPlanTask : rootPlanTasks) {
          forkJoinPool.invoke(rootPlanTask);
        }
      }

      return new BlockingQueueIterator(results, forkJoinPool);
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
      //      planTaskWorkers.shutdown()

      // ToDo, make sure the individual tasks cancel when interrupted
      forkJoinPool.shutdownNow();
    }
  }

  static class BlockingQueueIterator implements CloseableIterator<FileScanTask> {
    private final BlockingQueue<FileScanTask> results;
    private final ForkJoinPool forkJoinPool;
    private FileScanTask next = null;

    public BlockingQueueIterator(BlockingQueue<FileScanTask> results, ForkJoinPool forkJoinPool) {
      this.results = results;
      this.forkJoinPool = forkJoinPool;
    }

    @Override
    public boolean hasNext() {
      if (next != null) {
        return true;
      }
      // Poll for next result, blocks if empty and planning ongoing
      while (!forkJoinPool.isQuiescent() || !results.isEmpty()) {
        try {
          next = results.poll(100, TimeUnit.MILLISECONDS); // waits for result
          if (next != null) {
            return true;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      return false;
    }

    @Override
    public FileScanTask next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      FileScanTask result = next;
      next = null;
      return result;
    }

    @Override
    public void close() throws IOException {
      // (todo any cleanup)
    }
  }
}
