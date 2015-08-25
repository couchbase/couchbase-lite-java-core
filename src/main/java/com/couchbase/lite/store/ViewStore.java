//
//  ViewStorage.java
//
//  Created by Hideki Itakura on 6/10/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
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
     * @param views An array of CBL_ViewStorage instances, always including the receiver.
     * @throws CouchbaseLiteException
     */
    //void updateIndexes(List<ViewStorage> views) throws CouchbaseLiteException;
    void updateIndex() throws CouchbaseLiteException;

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
