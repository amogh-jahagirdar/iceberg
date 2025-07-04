---
title: "Configuration"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Spark Configuration

## Catalogs

Spark adds an API to plug in table catalogs that are used to load, create, and manage Iceberg tables. Spark catalogs are configured by setting Spark properties under `spark.sql.catalog`.

This creates an Iceberg catalog named `hive_prod` that loads tables from a Hive metastore:

```plain
spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hive_prod.type = hive
spark.sql.catalog.hive_prod.uri = thrift://metastore-host:port
# omit uri to use the same URI as Spark: hive.metastore.uris in hive-site.xml
```

Below is an example for a REST catalog named `rest_prod` that loads tables from REST URL `http://localhost:8080`:

```plain
spark.sql.catalog.rest_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.rest_prod.type = rest
spark.sql.catalog.rest_prod.uri = http://localhost:8080
```

Iceberg also supports a directory-based catalog in HDFS that can be configured using `type=hadoop`:

```plain
spark.sql.catalog.hadoop_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_prod.type = hadoop
spark.sql.catalog.hadoop_prod.warehouse = hdfs://nn:8020/warehouse/path
```

!!! info
    The Hive-based catalog only loads Iceberg tables. To load non-Iceberg tables in the same Hive metastore, use a [session catalog](#replacing-the-session-catalog).


### Catalog configuration

A catalog is created and named by adding a property `spark.sql.catalog.(catalog-name)` with an implementation class for its value.

Iceberg supplies two implementations:

* `org.apache.iceberg.spark.SparkCatalog` supports a Hive Metastore or a Hadoop warehouse as a catalog
* `org.apache.iceberg.spark.SparkSessionCatalog` adds support for Iceberg tables to Spark's built-in catalog, and delegates to the built-in catalog for non-Iceberg tables

Both catalogs are configured using properties nested under the catalog name. Common configuration properties for Hive and Hadoop are:

| Property                                           | Values                                             | Description                                                                                                                                                          |
| -------------------------------------------------- |----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.sql.catalog._catalog-name_.type              | `hive`, `hadoop`, `rest`, `glue`, `jdbc` or `nessie` | The underlying Iceberg catalog implementation, `HiveCatalog`, `HadoopCatalog`, `RESTCatalog`, `GlueCatalog`, `JdbcCatalog`, `NessieCatalog` or left unset if using a custom catalog |
| spark.sql.catalog._catalog-name_.catalog-impl      |                               | The custom Iceberg catalog implementation. If `type` is null, `catalog-impl` must not be null. |
| spark.sql.catalog._catalog-name_.io-impl                      |                               | The custom FileIO implementation. |
| spark.sql.catalog._catalog-name_.metrics-reporter-impl        |                               | The custom MetricsReporter implementation.  |
| spark.sql.catalog._catalog-name_.default-namespace | default                       | The default current namespace for the catalog |
| spark.sql.catalog._catalog-name_.uri               | thrift://host:port            | Hive metastore URL for hive typed catalog, REST URL for REST typed catalog |
| spark.sql.catalog._catalog-name_.warehouse         | hdfs://nn:8020/warehouse/path | Base path for the warehouse directory |
| spark.sql.catalog._catalog-name_.cache-enabled     | `true` or `false`             | Whether to enable catalog cache, default value is `true` |
| spark.sql.catalog._catalog-name_.cache.expiration-interval-ms | `30000` (30 seconds) | Duration after which cached catalog entries are expired; Only effective if `cache-enabled` is `true`. `-1` disables cache expiration and `0` disables caching entirely, irrespective of `cache-enabled`. Default is `30000` (30 seconds) |
| spark.sql.catalog._catalog-name_.table-default._propertyKey_  |                               | Default Iceberg table property value for property key _propertyKey_, which will be set on tables created by this catalog if not overridden                                                                                               |
| spark.sql.catalog._catalog-name_.table-override._propertyKey_ |                               | Enforced Iceberg table property value for property key _propertyKey_, which cannot be overridden on table creation by user                                                                                                               |
| spark.sql.catalog._catalog-name_.view-default._propertyKey_  |                               | Default Iceberg view property value for property key _propertyKey_, which will be set on views created by this catalog if not overridden                                                                                               |
| spark.sql.catalog._catalog-name_.view-override._propertyKey_ |                               | Enforced Iceberg view property value for property key _propertyKey_, which cannot be overridden on view creation by user                                                                                                               |
| spark.sql.catalog._catalog-name_.use-nullable-query-schema | `true` or `false` | Whether to preserve fields' nullability when creating the table using CTAS and RTAS. If set to `true`, all fields will be marked as nullable. If set to `false`, fields' nullability will be preserved. The default value is `true`. Available in Spark 3.5 and above.   |

Additional properties can be found in common [catalog configuration](configuration.md#catalog-properties).


### Using catalogs

Catalog names are used in SQL queries to identify a table. In the examples above, `hive_prod` and `hadoop_prod` can be used to prefix database and table names that will be loaded from those catalogs.

```sql
SELECT * FROM hive_prod.db.table; -- load db.table from catalog hive_prod
```

Spark 3 keeps track of the current catalog and namespace, which can be omitted from table names.

```sql
USE hive_prod.db;
SELECT * FROM table; -- load db.table from catalog hive_prod
```

To see the current catalog and namespace, run `SHOW CURRENT NAMESPACE`.

### Replacing the session catalog

To add Iceberg table support to Spark's built-in catalog, configure `spark_catalog` to use Iceberg's `SparkSessionCatalog`.

```plain
spark.sql.catalog.spark_catalog = org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type = hive
```

Spark's built-in catalog supports existing v1 and v2 tables tracked in a Hive Metastore. This configures Spark to use Iceberg's `SparkSessionCatalog` as a wrapper around that session catalog. When a table is not an Iceberg table, the built-in catalog will be used to load it instead.

This configuration can use same Hive Metastore for both Iceberg and non-Iceberg tables.

### Using catalog specific Hadoop configuration values

Similar to configuring Hadoop properties by using `spark.hadoop.*`, it's possible to set per-catalog Hadoop configuration values when using Spark by adding the property for the catalog with the prefix `spark.sql.catalog.(catalog-name).hadoop.*`. These properties will take precedence over values configured globally using `spark.hadoop.*` and will only affect Iceberg tables.

```plain
spark.sql.catalog.hadoop_prod.hadoop.fs.s3a.endpoint = http://aws-local:9000
```

### Loading a custom catalog

Spark supports loading a custom Iceberg `Catalog` implementation by specifying the `catalog-impl` property. Here is an example:

```plain
spark.sql.catalog.custom_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.custom_prod.catalog-impl = com.my.custom.CatalogImpl
spark.sql.catalog.custom_prod.my-additional-catalog-config = my-value
```

## SQL Extensions

Iceberg 0.11.0 and later add an extension module to Spark to add new SQL commands, like `CALL` for stored procedures or `ALTER TABLE ... WRITE ORDERED BY`.

Using those SQL commands requires adding Iceberg extensions to your Spark environment using the following Spark property:


| Spark extensions property | Iceberg extensions implementation                                   |
|---------------------------|---------------------------------------------------------------------|
| `spark.sql.extensions`    | `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` |

## Runtime configuration

### Precedence of Configuration Settings
Iceberg allows configurations to be specified at different levels. The effective configuration for a read or write operation is determined based on the following order of precedence:

1. DataSource API Read/Write Options – Explicitly passed to `.option(...)` in a read/write operation.

2. Spark Session Configuration - Set globally in Spark via `spark.conf.set(...)`, `spark-defaults.conf`, or `--conf` in spark-submit.

3. Table Properties – Defined on the Iceberg table via `ALTER TABLE SET TBLPROPERTIES`.

4. Default Value.

If a setting is not defined at a higher level, the next level is used as fallback. This allows flexibility while enabling global defaults when needed.

### Spark SQL Options

Iceberg supports setting various global behaviors using Spark SQL configuration options. These can be set via `spark.conf`, `SparkSession` settings, or Spark submit arguments.
For example:

```scala
// disabling vectorization
val spark = SparkSession.builder()
  .appName("IcebergExample")
  .master("local[*]")
  .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.iceberg.vectorization.enabled", "false")
  .getOrCreate()
```

| Spark option                                           | Default                                                        | Description                                                                                                                     |
|--------------------------------------------------------|----------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| spark.sql.iceberg.vectorization.enabled                | Table default                                                  | Enables vectorized reads of data files                                                                                          |
| spark.sql.iceberg.parquet.reader-type                  | ICEBERG                                                        | Sets Parquet reader implementation (`ICEBERG`,`COMET`)                                                                          |
| spark.sql.iceberg.check-nullability                    | true                                                           | Validate that the write schema's nullability matches the table's nullability                                                    |
| spark.sql.iceberg.check-ordering                       | true                                                           | Validates the write schema column order matches the table schema order                                                          |
| spark.sql.iceberg.planning.preserve-data-grouping      | false                                                          | When true, co-locate scan tasks for the same partition in the same read split, used in Storage Partitioned Joins                |
| spark.sql.iceberg.aggregate-push-down.enabled          | true                                                           | Enables pushdown of aggregate functions (MAX, MIN, COUNT)                                                                       |
| spark.sql.iceberg.distribution-mode                    | See [Spark Writes](spark-writes.md#writing-distribution-modes) | Controls distribution strategy during writes                                                                                    |
| spark.wap.id                                           | null                                                           | [Write-Audit-Publish](branching.md#audit-branch) snapshot staging ID                                                            |
| spark.wap.branch                                       | null                                                           | WAP branch name for snapshot commit                                                                                             |
| spark.sql.iceberg.compression-codec                    | Table default                                                  | Write compression codec (e.g., `zstd`, `snappy`)                                                                                |
| spark.sql.iceberg.compression-level                    | Table default                                                  | Compression level for Parquet/Avro                                                                                              |
| spark.sql.iceberg.compression-strategy                 | Table default                                                  | Compression strategy for ORC                                                                                                    |
| spark.sql.iceberg.data-planning-mode                   | AUTO                                                           | Scan planning mode for data files (`AUTO`, `LOCAL`, `DISTRIBUTED`)                                                              |
| spark.sql.iceberg.delete-planning-mode                 | AUTO                                                           | Scan planning mode for delete files (`AUTO`, `LOCAL`, `DISTRIBUTED`)                                                            |
| spark.sql.iceberg.advisory-partition-size              | Table default                                                  | Advisory size (bytes) used for writing to the Table when Spark's Adaptive Query Execution is enabled. Used to size output files |
| spark.sql.iceberg.locality.enabled                     | false                                                          | Report locality information for Spark task placement on executors                                                               |
| spark.sql.iceberg.executor-cache.enabled               | true                                                           | Enables cache for executor-side (currently used to cache Delete Files)                                                          |
| spark.sql.iceberg.executor-cache.timeout               | 10                                                             | Timeout in minutes for executor cache entries                                                                                   |
| spark.sql.iceberg.executor-cache.max-entry-size        | 67108864 (64MB)                                                | Max size per cache entry (bytes)                                                                                                |
| spark.sql.iceberg.executor-cache.max-total-size        | 134217728 (128MB)                                              | Max total executor cache size (bytes)                                                                                           |
| spark.sql.iceberg.executor-cache.locality.enabled      | false                                                          | Enables locality-aware executor cache usage                                                                                     |
| spark.sql.iceberg.merge-schema                         | false                                                          | Enables modifying the table schema to match the write schema. Only adds columns missing columns                                 |
| spark.sql.iceberg.report-column-stats                  | true                                                           | Report Puffin Table Statistics if available to Spark's Cost Based Optimizer. CBO must be enabled for this to be effective       |

### Read options

Spark read options are passed when configuring the DataFrameReader, like this:

```scala
// time travel
spark.read
    .option("snapshot-id", 10963874102873L)
    .table("catalog.db.table")
```

| Spark option    | Default               | Description                                                                               |
| --------------- | --------------------- | ----------------------------------------------------------------------------------------- |
| snapshot-id     | (latest)              | Snapshot ID of the table snapshot to read                                                 |
| as-of-timestamp | (latest)              | A timestamp in milliseconds; the snapshot used will be the snapshot current at this time. |
| split-size      | As per table property | Overrides this table's read.split.target-size and read.split.metadata-target-size         |
| lookback        | As per table property | Overrides this table's read.split.planning-lookback                                       |
| file-open-cost  | As per table property | Overrides this table's read.split.open-file-cost                                          |
| vectorization-enabled  | As per table property | Overrides this table's read.parquet.vectorization.enabled                                          |
| batch-size  | As per table property | Overrides this table's read.parquet.vectorization.batch-size                                          |
| stream-from-timestamp | (none) | A timestamp in milliseconds to stream from; if before the oldest known ancestor snapshot, the oldest will be used |
| streaming-max-files-per-micro-batch | INT_MAX | Maximum number of files per microbatch |
| streaming-max-rows-per-micro-batch  | INT_MAX | Maximum number of rows per microbatch |

!!! warning
    streaming-max-rows-per-micro-batch option sets a “soft max”, a batch will always include all the rows in the next unprocessed data file but additional files will not be included if doing so would exceed the soft max limit.



### Write options

Spark write options are passed when configuring the DataFrameWriterV2, like this:

```scala
// write with Avro instead of Parquet
df.writeTo("catalog.db.table")
    .option("write-format", "avro")
    .option("snapshot-property.key", "value")
    .append()
```

| Spark option           | Default                    | Description                                                  |
| ---------------------- | -------------------------- | ------------------------------------------------------------ |
| write-format           | Table write.format.default | File format to use for this write operation; parquet, avro, or orc |
| target-file-size-bytes | As per table property      | Overrides this table's write.target-file-size-bytes          |
| check-nullability      | true                       | Sets the nullable check on fields                            |
| snapshot-property._custom-key_    | null            | Adds an entry with custom-key and corresponding value in the snapshot summary (the `snapshot-property.` prefix is only required for DSv2)  |
| fanout-enabled       | false        | Overrides this table's write.spark.fanout.enabled  |
| check-ordering       | true        | Checks if input schema and table schema are same  |
| isolation-level | null | Desired isolation level for Dataframe overwrite operations.  `null` => no checks (for idempotent writes), `serializable` => check for concurrent inserts or deletes in destination partitions, `snapshot` => checks for concurrent deletes in destination partitions. |
| validate-from-snapshot-id | null | If isolation level is set, id of base snapshot from which to check concurrent write conflicts into a table. Should be the snapshot before any reads from the table. Can be obtained via [Table API](api.md#table-metadata) or [Snapshots table](spark-queries.md#snapshots). If null, the table's oldest known snapshot is used. |
| compression-codec      | Table write.(fileformat).compression-codec | Overrides this table's compression codec for this write      |
| compression-level      | Table write.(fileformat).compression-level | Overrides this table's compression level for Parquet and Avro tables for this write |
| compression-strategy   | Table write.orc.compression-strategy       | Overrides this table's compression strategy for ORC tables for this write |
| distribution-mode | See [Spark Writes](spark-writes.md#writing-distribution-modes) for defaults | Override this table's distribution mode for this write |
| delete-granularity | file | Override this table's delete granularity for this write |

CommitMetadata provides an interface to add custom metadata to a snapshot summary during a SQL execution, which can be beneficial for purposes such as auditing or change tracking. If properties start with `snapshot-property.`, then that prefix will be removed from each property. Here is an example:

```java
import org.apache.iceberg.spark.CommitMetadata;

Map<String, String> properties = Maps.newHashMap();
properties.put("property_key", "property_value");
CommitMetadata.withCommitProperties(properties,
        () -> {
            spark.sql("DELETE FROM " + tableName + " where id = 1");
            return 0;
        },
        RuntimeException.class);
```
