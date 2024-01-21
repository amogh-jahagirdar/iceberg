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
package org.apache.iceberg.io;

import java.util.List;

public class BulkDeletionFailureException extends RuntimeException {
  private final int numberFailedObjects;
  private List<String> failedFiles = null;

  public BulkDeletionFailureException(int numberFailedObjects) {
    super(String.format("Failed to delete %d files", numberFailedObjects));
    this.numberFailedObjects = numberFailedObjects;
  }

  public BulkDeletionFailureException(List<String> failedFiles) {
    this.numberFailedObjects = failedFiles.size();
    this.failedFiles = failedFiles;
  }

  public int numberFailedObjects() {
    return numberFailedObjects;
  }

  public List<String> failedObjects() {
    return failedFiles;
  }
}
