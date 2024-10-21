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
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.util.JsonUtil;

public class FetchScanTasksResponseParser {
  private static final String PLAN_TASKS = "plan-tasks";
  private static final String FILE_SCAN_TASKS = "file-scan-tasks";
  private static final String DELETE_FILES = "delete-files";

  private FetchScanTasksResponseParser() {}

  public static String toJson(FetchScanTasksResponse response) {
    // TODO need to pass specByIds
    return toJson(response, false);
  }

  public static String toJson(FetchScanTasksResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static void toJson(FetchScanTasksResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid response: fetchScanTasksResponse null");

    if (response.planTasks() == null && response.fileScanTasks() == null) {
      throw new IllegalArgumentException(
          "Invalid response: planTasks and fileScanTask can not both be null");
    }

    if ((response.deleteFiles() != null && !response.deleteFiles().isEmpty())
        && (response.fileScanTasks() == null || response.fileScanTasks().isEmpty())) {
      throw new IllegalArgumentException(
          "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
    }

    gen.writeStartObject();
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
        ContentFileParser.unboundContentFileToJson(
            deleteFiles.get(i), response.specsById().get(deleteFile.specId()), gen);
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
        PartitionSpec partitionSpec = response.specsById().get(fileScanTask.file().specId());
        RESTFileScanTaskParser.toJson(fileScanTask, deleteFileReferences, partitionSpec, gen);
      }
      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  public static FetchScanTasksResponse fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse fetchScanTasks response from null");
    return JsonUtil.parse(json, FetchScanTasksResponseParser::fromJson);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static FetchScanTasksResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(), "Invalid response: fetchScanTasksResponse null");

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
        // TODO we dont have caseSensitive flag at serial/deserialize time
        FileScanTask fileScanTask =
            (FileScanTask) RESTFileScanTaskParser.fromJson(fileScanTaskNode, allDeleteFiles);
        fileScanTaskBuilder.add(fileScanTask);
      }
      fileScanTasks = fileScanTaskBuilder.build();
    }

    if (planTasks == null && fileScanTasks == null) {
      throw new IllegalArgumentException(
          "Invalid response: planTasks and fileScanTask can not both be null");
    }

    if ((allDeleteFiles != null && !allDeleteFiles.isEmpty())
        && (fileScanTasks == null || fileScanTasks.isEmpty())) {
      throw new IllegalArgumentException(
          "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
    }

    return new FetchScanTasksResponse.Builder()
        .withPlanTasks(planTasks)
        .withFileScanTasks(fileScanTasks)
        .withDeleteFiles(allDeleteFiles)
        .build();
  }
}
