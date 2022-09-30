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
package org.apache.iceberg.catalog;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewRepresentation;

/** A Catalog API for view create, drop, and load operations. */
public interface ViewCatalog {

  /**
   * Return the name for this catalog.
   *
   * @return this catalog's name
   */
  String name();

  /**
   * Return all the identifiers under this namespace.
   *
   * @param namespace a namespace
   * @return a list of identifiers for views
   * @throws NotFoundException if the namespace is not found
   */
  List<TableIdentifier> listViews(Namespace namespace);

  /**
   * Load a view.
   *
   * @param identifier a view identifier
   * @return instance of {@link View} implementation referred by the identifier
   * @throws NoSuchViewException if the view does not exist
   */
  View loadView(TableIdentifier identifier);

  /**
   * Check whether view exists.
   *
   * @param identifier a view identifier
   * @return true if the view exists, false otherwise
   * @throws NoSuchViewException if the view does not exist
   */
  default boolean viewExists(TableIdentifier identifier) {
    try {
      loadView(identifier);
      return true;
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  /**
   * Create a view.
   *
   * @param identifier a view identifier
   * @param representations a list of view representations
   * @param properties a string map of view properties
   * @throws AlreadyExistsException if the view already exists
   */
  default View createView(
      TableIdentifier identifier,
      List<ViewRepresentation> representations,
      Map<String, String> properties) {
    return createView(identifier, null, representations, properties);
  }


  /**
   * Create a view with the provided schema
   *
   * @param identifier a view identifier
   * @param schema schema for the view
   * @param representations a list of view representations
   * @param properties a string map of view properties
   *
   * @throws AlreadyExistsException if the view already exists
   */
  View createView(
      TableIdentifier identifier,
      Schema schema,
      List<ViewRepresentation> representations,
      Map<String, String> properties);


  /**
   * Updates the current view with new representations.
   *
   * New representations will replace existing representations of the same type and dialect.
   * New representations with new types and dialects will be added to the current view representations.
   *
   * @param identifier a view identifier
   * @param newRepresentations a list of the new representations
   */
  View updateViewRepresentations(TableIdentifier identifier, List<ViewRepresentation> newRepresentations);

  /**
   * Replace a view.
   *
   * @param identifier a view identifier
   * @param representations a list of view representations
   * @param properties a string map of view properties
   */
  View replaceView(
      TableIdentifier identifier,
      List<ViewRepresentation> representations,
      Map<String, String> properties);

  /**
   * Drop a view and delete all metadata files.
   *
   * @param identifier a view identifier
   * @return true if the view was dropped, false if the view did not exist
   */
  default boolean dropView(TableIdentifier identifier) {
    return dropView(identifier, true /* drop metadata files */);
  }

  /**
   * Drop a view; optionally delete metadata files.
   *
   * <p>If purge is set to true the implementation should delete all metadata files.
   *
   * @param identifier a view identifier
   * @param purge if true, delete all metadata files in the view
   * @return true if the view was dropped, false if the view did not exist
   */
  boolean dropView(TableIdentifier identifier, boolean purge);

  /**
   * Rename a view.
   *
   * @param from identifier of the view to rename
   * @param to new view identifier
   * @throws NoSuchViewException if the "from" view does not exist
   * @throws AlreadyExistsException if the "to" view already exists
   */
  void renameView(TableIdentifier from, TableIdentifier to);

  /**
   * Invalidate cached view metadata from current catalog.
   *
   * <p>If the view is already loaded or cached, drop cached data. If the view does not exist or is
   * not cached, do nothing.
   *
   * @param identifier a view identifier
   */
  default void invalidateView(TableIdentifier identifier) {}

  /**
   * Initialize a view catalog given a custom name and a map of catalog properties.
   *
   * <p>A custom view catalog implementation must have a no-arg constructor. A compute engine like
   * Spark or Flink will first initialize the catalog without any arguments, and then call this
   * method to complete catalog initialization with properties passed into the engine.
   *
   * @param name a custom name for the catalog
   * @param properties catalog properties
   */
  default void initialize(String name, Map<String, String> properties) {}
}
