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

interface V4ManifestEntry {

  ManifestEntry.Status status();

  ManifestContent contentType();

  V4ContentEntry content();

  byte[] dvContent();

  Long sequenceNumber();

  Long minSequenceNumber();

  Long snapshotId();

  class EntryV4 implements V4ManifestEntry, StructLike {
    private ManifestEntry.Status status;
    private Long snapshotId;
    private ManifestContent contentType;
    private byte[] dvContent;
    private V4ContentEntry content;
    private Long sequenceNumber;
    private Long minSequenceNumber;

    protected EntryV4() {
      this.content = new V4ContentEntry();
    }

    @Override
    public ManifestEntry.Status status() {
      return status;
    }

    public EntryV4 status(ManifestEntry.Status status) {
      this.status = status;
      return this;
    }

    @Override
    public ManifestContent contentType() {
      return contentType;
    }

    @Override
    public V4ContentEntry content() {
      return content;
    }

    @Override
    public byte[] dvContent() {
      return dvContent;
    }

    public EntryV4 content(V4ContentEntry content) {
      this.content = content;
      return this;
    }

    public EntryV4 dvContent(byte[] dvContent) {
      this.dvContent = dvContent;
      return this;
    }

    public EntryV4 contentType(ManifestContent contentType) {
      this.contentType = contentType;
      return this;
    }

    @Override
    public Long sequenceNumber() {
      return sequenceNumber;
    }

    public EntryV4 sequenceNumber(Long sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
      return this;
    }

    @Override
    public Long minSequenceNumber() {
      return sequenceNumber;
    }

    public EntryV4 minSequenceNumber(Long sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
      return this;
    }

    @Override
    public Long snapshotId() {
      return snapshotId;
    }

    public EntryV4 snapshotId(Long snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    private Object get(int pos) {
      switch (pos) {
        case 0:
          return status.id();
        case 1:
          return snapshotId;
        case 2:
          return contentType.id();
        case 3:
          return dvContent;
        case 4:
          return content;
        case 5:
          return sequenceNumber;
        case 6:
          return minSequenceNumber;

        default:
          return null;
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException();
    }
  }

  //  static EntryV4 create(DataFile entry) {
  //    return new EntryV4()
  //        .content().location(entry.location())
  //        .contentSize(entry.fileSizeInBytes())
  //        .partitionSpecId(entry.specId())
  //        .sequenceNumber(entry.dataSequenceNumber())
  //        .minSequenceNumber(entry.fileSequenceNumber())
  //        .offsets(entry.splitOffsets())
  //        .equalityIds(entry.equalityFieldIds())
  //        .firstRowId(entry.firstRowId());
  //  }
  //
  //  static EntryV4 create(DeleteFile entry) {
  //    return new EntryV4()
  //        .path(entry.location())
  //        .length(entry.fileSizeInBytes())
  //        .specId(entry.specId())
  //        .content(ManifestContent.DELETES)
  //        .sequenceNumber(entry.dataSequenceNumber())
  //        .minSequenceNumber(entry.fileSequenceNumber())
  //        .offsets(entry.splitOffsets())
  //        .equalityIds(entry.equalityFieldIds())
  //        .firstRowId(entry.firstRowId());
  //  }
  //
  //  static EntryV4 create(ManifestFile entry) {
  //    return new EntryV4()
  //        .path(entry.path())
  //        .length(entry.length())
  //        .specId(entry.partitionSpecId())
  //        .content(ManifestContent.MANIFESTS)
  //        .sequenceNumber(entry.sequenceNumber())
  //        .minSequenceNumber(entry.minSequenceNumber())
  //        .firstRowId(entry.firstRowId());
  //  }
}
