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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

/** Base class logic for files metadata tables */
abstract class BaseFilesTable extends BaseMetadataTable {

  BaseFilesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public Schema schema() {
    StructType partitionType = Partitioning.partitionType(table());
    Schema schema = new Schema(DataFile.getType(partitionType).fields());
    if (partitionType.fields().isEmpty()) {
      // avoid returning an empty struct, which is not always supported.
      // instead, drop the partition field
      schema = TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    }

    return TypeUtil.join(schema, MetricsUtil.readableMetricsSchema(table().schema(), schema));
  }

  private static CloseableIterable<FileScanTask> planFiles(
      Table table,
      CloseableIterable<ManifestFile> manifests,
      Schema tableSchema,
      Schema projectedSchema,
      TableScanContext context) {
    Expression rowFilter = context.rowFilter();
    boolean caseSensitive = context.caseSensitive();
    boolean ignoreResiduals = context.ignoreResiduals();

    LoadingCache<Integer, ManifestEvaluator> evalCache =
        Caffeine.newBuilder()
            .build(
                specId -> {
                  PartitionSpec spec = table.specs().get(specId);
                  PartitionSpec transformedSpec = BaseFilesTable.transformSpec(tableSchema, spec);
                  return ManifestEvaluator.forRowFilter(rowFilter, transformedSpec, caseSensitive);
                });

    // v4 manifests can track files from more than one spec and have no partition summaries
    CloseableIterable<ManifestFile> filteredManifests =
        CloseableIterable.filter(
            manifests,
            manifest ->
                isV4Manifest(manifest) || evalCache.get(manifest.partitionSpecId()).eval(manifest));

    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;

    return CloseableIterable.transform(
        filteredManifests,
        manifest -> new ManifestReadTask(table, manifest, projectedSchema, filter));
  }

  private static boolean isV4Manifest(ManifestFile manifest) {
    return manifest.formatVersion() >= V4Metadata.FORMAT_VERSION;
  }

  abstract static class BaseFilesTableScan extends BaseMetadataTableScan {

    protected BaseFilesTableScan(Table table, Schema schema, MetadataTableType tableType) {
      super(table, schema, tableType);
    }

    protected BaseFilesTableScan(
        Table table, Schema schema, MetadataTableType tableType, TableScanContext context) {
      super(table, schema, tableType, context);
    }

    /** Returns an iterable of manifest files to explore for this files metadata table scan */
    protected abstract CloseableIterable<ManifestFile> manifests();

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      return BaseFilesTable.planFiles(table(), manifests(), tableSchema(), schema(), context());
    }
  }

  abstract static class BaseAllFilesTableScan extends BaseAllMetadataTableScan {

    protected BaseAllFilesTableScan(Table table, Schema schema, MetadataTableType tableType) {
      super(table, schema, tableType);
    }

    protected BaseAllFilesTableScan(
        Table table, Schema schema, MetadataTableType tableType, TableScanContext context) {
      super(table, schema, tableType, context);
    }

    /** Returns an iterable of manifest files to explore for this all files metadata table scan */
    protected abstract CloseableIterable<ManifestFile> manifests();

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      return BaseFilesTable.planFiles(table(), manifests(), tableSchema(), schema(), context());
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {

    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private final ManifestFile manifest;
    private final Schema dataTableSchema;
    private final Schema projection;

    private ManifestReadTask(
        Table table, ManifestFile manifest, Schema projection, Expression filter) {
      this(table.schema(), table.io(), table.specs(), manifest, projection, filter);
    }

    ManifestReadTask(
        Schema dataTableSchema,
        FileIO io,
        Map<Integer, PartitionSpec> specsById,
        ManifestFile manifest,
        Schema projection,
        Expression filter) {
      super(
          DataFiles.fromManifest(manifest),
          null,
          SchemaParser.toJson(projection),
          PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
          ResidualEvaluator.unpartitioned(filter));
      this.io = io;
      this.specsById = Maps.newHashMap(specsById);
      this.manifest = manifest;
      this.dataTableSchema = dataTableSchema;
      this.projection = projection;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      Types.NestedField readableMetricsField = projection.findField(MetricsUtil.READABLE_METRICS);

      if (readableMetricsField == null) {
        return CloseableIterable.transform(files(projection), file -> (StructLike) file);
      } else {

        Schema actualProjection = projectionForReadableMetrics(projection, readableMetricsField);
        return CloseableIterable.transform(
            files(actualProjection), f -> withReadableMetrics(f, readableMetricsField));
      }
    }

    @Override
    public long estimatedRowsCount() {
      return (long) manifest.addedFilesCount()
          + (long) manifest.deletedFilesCount()
          + (long) manifest.existingFilesCount();
    }

    private CloseableIterable<? extends ContentFile<?>> files(Schema fileProjection) {
      if (isV4Manifest(manifest)) {
        return v4Files(fileProjection);
      }

      return ManifestFiles.open(manifest, io, specsById).project(fileProjection);
    }

    private CloseableIterable<DataFile> v4Files(Schema fileProjection) {
      V4ManifestReader reader =
          V4ManifestReader.builder(manifest, io, dataTableSchema, specsById)
              .forScanPlanning()
              .projectStats(TypeUtil.getProjectedIds(dataTableSchema))
              .build();
      return CloseableIterable.transform(reader, file -> projectedDataFile(file, fileProjection));
    }

    // rows must be structs in the order of the projection
    private DataFile projectedDataFile(TrackedFile file, Schema fileProjection) {
      DataFile dataFile = TrackedFileAdapters.asDataFile(file, specsById);
      PartitionSpec spec = specsById.get(dataFile.specId());
      StructLike fullFile = (StructLike) DataFiles.builder(spec).copy(dataFile).build();
      StructType fullType = DataFile.getType(spec.partitionType());

      GenericDataFile projected = new GenericDataFile(fileProjection.asStruct());
      List<Types.NestedField> fields = fileProjection.columns();
      for (int pos = 0; pos < fields.size(); pos += 1) {
        Types.NestedField fullField = fullType.field(fields.get(pos).fieldId());
        projected.set(pos, fullFile.get(fullType.fields().indexOf(fullField), Object.class));
      }

      return projected;
    }

    /**
     * Given content file metadata, append a 'readable_metrics' column that return the file's
     * metrics in human-readable form.
     *
     * @param file content file metadata
     * @param readableMetricsField projected "readable_metrics" field
     * @return struct representing content file, with appended readable_metrics field
     */
    private StructLike withReadableMetrics(
        ContentFile<?> file, Types.NestedField readableMetricsField) {
      int structSize = projection.columns().size();
      MetricsUtil.ReadableMetricsStruct readableMetrics =
          readableMetrics(file, readableMetricsField);
      int metricsPosition = projection.columns().indexOf(readableMetricsField);

      return new MetricsUtil.StructWithReadableMetrics(
          (StructLike) file, structSize, readableMetrics, metricsPosition);
    }

    private MetricsUtil.ReadableMetricsStruct readableMetrics(
        ContentFile<?> file, Types.NestedField readableMetricsField) {
      StructType projectedMetricType = readableMetricsField.type().asStructType();
      return MetricsUtil.readableMetricsStruct(dataTableSchema, file, projectedMetricType);
    }

    /**
     * Create a projection on content files metadata by removing virtual 'readable_column' and
     * ensuring that the underlying metrics used to create that column are part of the final
     * projection.
     *
     * @param requestedProjection requested projection
     * @param readableMetricsField readable_metrics field
     * @return actual projection to be used
     */
    private Schema projectionForReadableMetrics(
        Schema requestedProjection, Types.NestedField readableMetricsField) {
      Set<Integer> readableMetricsIds = TypeUtil.getProjectedIds(readableMetricsField.type());
      Schema realProjection = TypeUtil.selectNot(requestedProjection, readableMetricsIds);

      Schema requiredMetricsColumns =
          new Schema(
              MetricsUtil.READABLE_METRIC_COLS.stream()
                  .map(MetricsUtil.ReadableMetricColDefinition::originalCol)
                  .collect(Collectors.toList()));
      return TypeUtil.join(realProjection, requiredMetricsColumns);
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }

    FileIO io() {
      return io;
    }

    Map<Integer, PartitionSpec> specsById() {
      return specsById;
    }

    ManifestFile manifest() {
      return manifest;
    }

    Schema dataTableSchema() {
      return dataTableSchema;
    }

    Schema projection() {
      return projection;
    }
  }
}
