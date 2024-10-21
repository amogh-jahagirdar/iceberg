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
package org.apache.iceberg.rest.responses;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RESTFileScanTaskParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.JsonUtil;

class TableScanResponseParser {

  private TableScanResponseParser() {}

  static final String FILE_SCAN_TASKS = "file-scan-tasks";
  static final String DELETE_FILES = "delete-files";

  public static List<DeleteFile> parseDeleteFiles(JsonNode node) {
    if (node.has(DELETE_FILES)) {
      JsonNode deleteFiles = JsonUtil.get(DELETE_FILES, node);
      Preconditions.checkArgument(
          deleteFiles.isArray(), "Cannot parse delete files from non-array: %s", deleteFiles);
      ImmutableList.Builder<DeleteFile> deleteFilesBuilder = ImmutableList.builder();
      for (JsonNode deleteFileNode : deleteFiles) {
        DeleteFile deleteFile =
            (DeleteFile) ContentFileParser.unboundContentFileFromJson(deleteFileNode);
        deleteFilesBuilder.add(deleteFile);
      }
      return deleteFilesBuilder.build();
    }

    return null;
  }

  public static List<FileScanTask> parseFileScanTasks(JsonNode node, List<DeleteFile> deleteFiles) {
    if (node.has(FILE_SCAN_TASKS)) {
      JsonNode scanTasks = JsonUtil.get(FILE_SCAN_TASKS, node);
      Preconditions.checkArgument(
          scanTasks.isArray(), "Cannot parse file scan tasks from non-array: %s", scanTasks);
      ImmutableList.Builder<FileScanTask> fileScanTaskBuilder = ImmutableList.builder();
      for (JsonNode fileScanTaskNode : scanTasks) {
        FileScanTask fileScanTask = RESTFileScanTaskParser.fromJson(fileScanTaskNode, deleteFiles);
        fileScanTaskBuilder.add(fileScanTask);
      }

      return fileScanTaskBuilder.build();
    }

    return null;
  }

  public static void serializeScanTasks(
      List<FileScanTask> fileScanTasks,
      List<DeleteFile> deleteFiles,
      Map<Integer, PartitionSpec> specsById,
      JsonGenerator gen)
      throws IOException {
    Map<String, Integer> deleteFilePathToIndex = Maps.newHashMap();
    if (deleteFiles != null) {
      Preconditions.checkArgument(
          specsById != null, "Cannot serialize response without specs by ID defined");
      gen.writeArrayFieldStart(DELETE_FILES);
      for (int i = 0; i < deleteFiles.size(); i++) {
        DeleteFile deleteFile = deleteFiles.get(i);
        deleteFilePathToIndex.put(String.valueOf(deleteFile.path()), i);
        ContentFileParser.unboundContentFileToJson(
            deleteFiles.get(i), specsById.get(deleteFile.specId()), gen);
      }
      gen.writeEndArray();
    }

    if (fileScanTasks != null) {
      gen.writeArrayFieldStart(FILE_SCAN_TASKS);
      Set<Integer> deleteFileReferences = Sets.newHashSet();
      for (FileScanTask fileScanTask : fileScanTasks) {
        if (deleteFiles != null) {
          for (DeleteFile taskDelete : fileScanTask.deletes()) {
            deleteFileReferences.add(deleteFilePathToIndex.get(taskDelete.path().toString()));
          }
        }

        PartitionSpec partitionSpec = specsById.get(fileScanTask.file().specId());
        RESTFileScanTaskParser.toJson(fileScanTask, deleteFileReferences, partitionSpec, gen);
      }
      gen.writeEndArray();
    }
  }
}
