/*
 * Licensed to the Apache Software Foundation (ASF) licenses this file
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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;
import scala.collection.JavaConverters;

public class TestStreamingWithRESTCatalog {

  private static SparkSession spark;

  @TempDir private Path temp;

  @BeforeAll
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession current = spark;
    spark = null;
    if (current != null) {
      current.stop();
    }
  }

  /**
   * End-to-end test: a Spark Streaming append through a real Jetty HTTP server where the first
   * commit attempt receives a 409 (CommitFailedException), triggering a retry that succeeds. After
   * the retry, no files (manifest list, manifests, data files) are cleaned up.
   */
  @Test
  public void testStreamingNoCleanupAfterCommitFailedRetrySuccess() throws Exception {
    AtomicBoolean firstAttempt = new AtomicBoolean(true);

    // Use JdbcCatalog (SQLite) backend so data files land on a real local filesystem
    Map<String, String> backendConfig =
        ImmutableMap.of(
            CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName(),
            CatalogProperties.URI, "jdbc:sqlite:" + temp.resolve("iceberg.db"),
            "jdbc.schema-version", "V1",
            CatalogProperties.WAREHOUSE_LOCATION, temp.resolve("warehouse").toString());
    Catalog backendCatalog =
        CatalogUtil.buildIcebergCatalog("test", backendConfig, new Configuration());

    // Custom adapter: on the first POST to a table-specific path (UPDATE_TABLE), respond with a
    // 409 CommitFailedException via the error handler rather than delegating to the backend.
    // Subsequent calls delegate normally, allowing the retry to succeed.
    RESTCatalogAdapter adapter =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          protected <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            // UPDATE_TABLE: POST .../namespaces/{ns}/tables/{table}  (path contains "/tables/")
            // CREATE_TABLE: POST .../namespaces/{ns}/tables           (path ends with "/tables")
            boolean isUpdateTable =
                request.method() == HTTPRequest.HTTPMethod.POST
                    && request.path().matches(".*tables/[^/]+$");
            if (isUpdateTable && firstAttempt.getAndSet(false)) {
              errorHandler.accept(
                  ErrorResponse.builder()
                      .responseCode(409)
                      .withType("CommitFailedException")
                      .withMessage("Simulated conflict: metadata has changed")
                      .build());
              return null;
            }
            return super.execute(request, responseType, errorHandler, responseHeaders);
          }
        };

    // Start Jetty server
    ServletContextHandler context =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addServlet(new ServletHolder(new RESTCatalogServlet(adapter)), "/*");

    Server httpServer =
        new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(context);
    httpServer.start();

    try {
      int port = ((ServerConnector) httpServer.getConnectors()[0]).getLocalPort();
      String serverUri =
          "http://" + InetAddress.getLoopbackAddress().getHostAddress() + ":" + port;

      // Configure Spark to use the REST catalog pointing at our Jetty server
      spark.conf().set("spark.sql.catalog.testrest", SparkCatalog.class.getName());
      spark.conf().set("spark.sql.catalog.testrest.type", "rest");
      spark.conf().set("spark.sql.catalog.testrest.uri", serverUri);
      spark.conf().set("spark.sql.catalog.testrest.cache-enabled", "false");

      spark.sql("CREATE NAMESPACE IF NOT EXISTS testrest.default");
      spark.sql("CREATE TABLE testrest.default.test_table (id INT, data STRING) USING iceberg");

      // Run a streaming batch: first commit attempt gets 409, retry succeeds
      MemoryStream<Integer> inputStream = newMemoryStream(1, spark, Encoders.INT());
      DataStreamWriter<Row> streamWriter =
          inputStream
              .toDF()
              .selectExpr("value AS id", "CAST(value AS STRING) AS data")
              .writeStream()
              .outputMode("append")
              .format("iceberg")
              .option("checkpointLocation", temp.resolve("checkpoint").toString())
              .option("path", "testrest.default.test_table");

      StreamingQuery query = streamWriter.start();
      send(Lists.newArrayList(1, 2, 3), inputStream);
      query.processAllAvailable();
      query.stop();

      // Verify the fault injection actually fired; if firstAttempt is still true the path
      // detection did not match and no 409 was injected, meaning the test is not valid.
      assertThat(firstAttempt.get())
          .as("fault injection should have fired on the first commit attempt")
          .isFalse();

      // Load committed state via a fresh REST catalog client
      try (RESTCatalog restCatalog = new RESTCatalog()) {
        restCatalog.setConf(new Configuration());
        restCatalog.initialize(
            "client", ImmutableMap.of(CatalogProperties.URI, serverUri));

        Table table = restCatalog.loadTable(TableIdentifier.of("default", "test_table"));
        Snapshot snapshot = table.currentSnapshot();
        assertThat(snapshot).isNotNull();

        // Verify manifest list exists (not cleaned up)
        assertThat(table.io().newInputFile(snapshot.manifestListLocation()).exists()).isTrue();

        // Verify every manifest file exists (not cleaned up)
        List<ManifestFile> manifests = snapshot.allManifests(table.io());
        for (ManifestFile manifest : manifests) {
          assertThat(table.io().newInputFile(manifest.path()).exists()).isTrue();
        }

        // Verify every data file exists (not cleaned up)
        List<DataFile> addedFiles = Lists.newArrayList(snapshot.addedDataFiles(table.io()));
        assertThat(addedFiles).isNotEmpty();
        for (DataFile dataFile : addedFiles) {
          assertThat(table.io().newInputFile(dataFile.path().toString()).exists()).isTrue();
        }
      }
    } finally {
      for (StreamingQuery q : spark.streams().active()) {
        q.stop();
      }
      httpServer.stop();
    }
  }

  private <T> MemoryStream<T> newMemoryStream(
      int id, SparkSession sparkSession, org.apache.spark.sql.Encoder<T> encoder) {
    return new MemoryStream<>(id, sparkSession, Option.empty(), encoder);
  }

  @SuppressWarnings("deprecation")
  private <T> void send(List<T> records, MemoryStream<T> stream) {
    stream.addData(JavaConverters.asScalaBuffer(records));
  }
}
