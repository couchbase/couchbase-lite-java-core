/**
 * Copyright (c) 2016 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.couchbase.lite.store;

import com.couchbase.lite.*;

import java.util.List;
import java.util.Map;

/**
 * Storage for a view. Instances are created by Storage implementations,
 * and are owned by View instances.
 */
public interface ViewStore {
    /**
     * The name of the view.
     */
    String getName();

    /**
     * The delegate (in practice, the owning View itself.)
     */
    ViewStoreDelegate getDelegate();

    void setDelegate(ViewStoreDelegate delegate);

    /**
     * Closes the storage.
     */
    void close();

    /**
     * Erases the view's _index.
     */
    void deleteIndex();

    /**
     * Deletes the view's storage (metadata and _index), removing it from the database.
     */
    void deleteView();

    /**
     * Updates the version of the view. A change in version means the delegate's map block has
     * changed its semantics, so the _index should be deleted.
     */
    boolean setVersion(String version);

    /**
     * The total number of rows in the _index.
     */
    int getTotalRows();

    /**
     * The last sequence number that has been indexed.
     */
    long getLastSequenceIndexed();

    /**
     * The last sequence number that caused an actual change in the _index.
     */
    long getLastSequenceChangedAt();

    /**
     * Updates the indexes of one or more views in parallel.
     *
     * @param views An array of ViewStore instances, always including the receiver.
     * @return Status OK if updated or NOT_MODIFIED if already up-to-date.
     * @throws CouchbaseLiteException
     */
    Status updateIndexes(List<ViewStore> views) throws CouchbaseLiteException;

    /**
     * Queries the view without performing any reducing or grouping.
     */
    List<QueryRow> regularQuery(QueryOptions options) throws CouchbaseLiteException;

    /**
     * Queries the view, with reducing or grouping as per the options.
     */
    List<QueryRow> reducedQuery(QueryOptions options) throws CouchbaseLiteException;

    /**
     * Methods for debugging
     */
    List<Map<String, Object>> dump();

    void setCollation(View.TDViewCollation collation);
}
