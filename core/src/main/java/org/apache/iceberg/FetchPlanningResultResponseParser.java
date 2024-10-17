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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.util.JsonUtil;

public class FetchPlanningResultResponseParser {
  private static final String PLAN_STATUS = "plan-status";
  private static final String PLAN_TASKS = "plan-tasks";
  private static final String FILE_SCAN_TASKS = "file-scan-tasks";
  private static final String DELETE_FILES = "delete-files";

  private FetchPlanningResultResponseParser() {}

  public static String toJson(FetchPlanningResultResponse response) {
    return toJson(response, false);
  }

  public static String toJson(FetchPlanningResultResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(FetchPlanningResultResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid response: planTableScanResponse null");

    gen.writeStartObject();
    if (response.planStatus() != null) {
      gen.writeStringField(PLAN_STATUS, response.planStatus().status());
    }

    if (response.planTasks() != null) {
      gen.writeArrayFieldStart(PLAN_TASKS);
      for (String planTask : response.planTasks()) {
        gen.writeString(planTask);
      }
      gen.writeEndArray();
    }

    List<DeleteFile> deleteFiles = null;
    Map<String, Integer> deleteFilePathToIndex = Maps.newHashMap();
    if (response.deleteFiles() != null) {
      deleteFiles = response.deleteFiles();
      gen.writeArrayFieldStart(DELETE_FILES);
      for (int i = 0; i < deleteFiles.size(); i++) {
        DeleteFile deleteFile = deleteFiles.get(i);
        deleteFilePathToIndex.put(String.valueOf(deleteFile.path()), i);
        ContentFileParser.toJson(
            deleteFiles.get(i), response.partitionSpecsById().get(deleteFile.specId()), gen);
      }
      gen.writeEndArray();
    }

    if (response.fileScanTasks() != null) {
      Set<Integer> deleteFileReferences = Sets.newHashSet();
      gen.writeArrayFieldStart(FILE_SCAN_TASKS);
      for (FileScanTask fileScanTask : response.fileScanTasks()) {
        if (deleteFiles != null) {
          for (DeleteFile taskDelete : fileScanTask.deletes()) {
            deleteFileReferences.add(deleteFilePathToIndex.get(taskDelete.path().toString()));
          }
        }
        RESTFileScanTaskParser.toJson(fileScanTask, deleteFileReferences, gen);
      }
      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  public static FetchPlanningResultResponse fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse plan table response from null");
    return JsonUtil.parse(json, FetchPlanningResultResponseParser::fromJson);
  }

  public static FetchPlanningResultResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(),
        "Cannot parse planTableScan response from empty or null object");

    PlanStatus planStatus = null;
    if (json.has(PLAN_STATUS)) {
      String status = JsonUtil.getString(PLAN_STATUS, json);
      planStatus = PlanStatus.fromName(status);
    }

    List<String> planTasks = JsonUtil.getStringListOrNull(PLAN_TASKS, json);

    List<DeleteFile> allDeleteFiles = null;
    if (json.has(DELETE_FILES)) {
      JsonNode deletesArray = json.get(DELETE_FILES);
      ImmutableList.Builder<DeleteFile> deleteFilesBuilder = ImmutableList.builder();
      for (JsonNode deleteFileNode : deletesArray) {
        DeleteFile deleteFile =
            (DeleteFile) ContentFileParser.unboundContentFileFromJson(deleteFileNode);
        deleteFilesBuilder.add(deleteFile);
      }
      allDeleteFiles = deleteFilesBuilder.build();
    }

    List<FileScanTask> fileScanTasks = null;
    if (json.has(FILE_SCAN_TASKS)) {
      JsonNode fileScanTasksArray = json.get(FILE_SCAN_TASKS);
      ImmutableList.Builder<FileScanTask> fileScanTaskBuilder = ImmutableList.builder();
      for (JsonNode fileScanTaskNode : fileScanTasksArray) {

        // TODO we dont have caseSensitive flag at serial/deserial time
        FileScanTask fileScanTask =
            (FileScanTask) RESTFileScanTaskParser.fromJson(fileScanTaskNode, allDeleteFiles);
        fileScanTaskBuilder.add(fileScanTask);
      }
      fileScanTasks = fileScanTaskBuilder.build();
    }

    return new FetchPlanningResultResponse.Builder()
        .withPlanStatus(planStatus)
        .withPlanTasks(planTasks)
        .withFileScanTasks(fileScanTasks)
        .withDeleteFiles(allDeleteFiles)
        .build();
  }
}
