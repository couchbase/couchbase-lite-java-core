/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 * <p/>
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.store.ViewStore;
import com.couchbase.lite.store.ViewStoreDelegate;
import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a view available in a database.
 */
public final class View implements ViewStoreDelegate {

    public enum TDViewCollation {
        TDViewCollationUnicode, TDViewCollationRaw, TDViewCollationASCII
    }

    // Defined in CBLView.h
    private Database database;
    private String name;
    private Mapper mapBlock;
    private Reducer reduceBlock;
    private String version; // TODO: iOS version store version information in CBL_Shared.
    private static ViewCompiler compiler;
    private ViewStore viewStore;

    ///////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////

    /*
    @InterfaceAudience.Private
    protected View(Database database, String name) {
        this.database = database;
        this.name = name;
        this.viewStore = database.getStore().getViewStorage(name, true);
        this.viewStore.setDelegate(this);
    }
    */
    @InterfaceAudience.Private
    protected View(Database database, String name, boolean create) throws CouchbaseLiteException{
        this.database = database;
        this.name = name;
        this.viewStore = database.getStore().getViewStorage(name, create);
        if(this.viewStore == null)
            throw new CouchbaseLiteException(Status.NOT_FOUND);
        this.viewStore.setDelegate(this);
    }
    ///////////////////////////////////////////////////////////////////////////
    // Implementation of ViewStorageDelegate
    ///////////////////////////////////////////////////////////////////////////

    /**
     * The map function that controls how index rows are created from documents.
     */
    @Override
    @InterfaceAudience.Public
    public Mapper getMap() {
        return mapBlock;
    }

    /**
     * The optional reduce function, which aggregates together multiple rows.
     */
    @Override
    @InterfaceAudience.Public
    public Reducer getReduce() {
        return reduceBlock;
    }

    @Override
    public String getMapVersion() {
        // TODO: Should be from CBL_Shared
        return version;
    }

    /**
     * If this property is set, only documents whose "type" property is equal to its value will be
     * passed to the map block and indexed. This can speed up indexing.
     * Just like the map block, this property is not persistent; it needs to be set at runtime before
     * the view is queried. And if its value changes, the view's version also needs to change.
     */
    @Override
    public String getDocumentType() {
        return null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // API (CBLView.h/CBLView.m)
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get the database that owns this view.
     */
    @InterfaceAudience.Private
    protected Database getDatabase() {
        return database;
    }

    /**
     * Get the name of the view.
     */
    @InterfaceAudience.Public
    public String getName() {
        return name;
    }

    /**
     * Defines a view's functions.
     * <p/>
     * The view's definition is given as a class that conforms to the Mapper or
     * Reducer interface (or null to delete the view). The body of the block
     * should call the 'emit' object (passed in as a paramter) for every key/value pair
     * it wants to write to the view.
     * <p/>
     * Since the function itself is obviously not stored in the database (only a unique
     * string idenfitying it), you must re-define the view on every launch of the app!
     * If the database needs to rebuild the view but the function hasn't been defined yet,
     * it will fail and the view will be empty, causing weird problems later on.
     * <p/>
     * It is very important that this block be a law-abiding map function! As in other
     * languages, it must be a "pure" function, with no side effects, that always emits
     * the same values given the same input document. That means that it should not access
     * or change any external state; be careful, since callbacks make that so easy that you
     * might do it inadvertently!  The callback may be called on any thread, or on
     * multiple threads simultaneously. This won't be a problem if the code is "pure" as
     * described above, since it will as a consequence also be thread-safe.
     */
    @InterfaceAudience.Public
    public boolean setMapReduce(Mapper mapBlock, Reducer reduceBlock, String version) {
        assert (mapBlock != null);
        assert (version != null);
        boolean changed = (this.version == null || !this.version.equals(version));
        this.mapBlock = mapBlock;
        this.reduceBlock = reduceBlock;
        this.version = version;
        viewStore.setVersion(version); // for SQLite
        return changed;
    }

    /**
     * Defines a view that has no reduce function.
     * See setMapReduce() for more information.
     */
    @InterfaceAudience.Public
    public boolean setMap(Mapper mapBlock, String version) {
        return setMapReduce(mapBlock, null, version);
    }

    /**
     * Is the view's index currently out of date?
     */
    @InterfaceAudience.Public
    public boolean isStale() {
        return (viewStore.getLastSequenceIndexed() < database.getLastSequenceNumber());
    }

    /**
     * Get the last sequence number indexed so far.
     */
    @InterfaceAudience.Public
    public long getLastSequenceIndexed() {
        return viewStore.getLastSequenceIndexed();
    }

    /**
     * Deletes the view's persistent index. It will be regenerated on the next query.
     */
    @InterfaceAudience.Public
    public void deleteIndex() {
        viewStore.deleteIndex();
    }

    /**
     * Deletes the view, persistently.
     * <p/>
     * NOTE: It should be - (void) deleteView;
     */
    @InterfaceAudience.Public
    public void delete() {
        if (viewStore != null)
            viewStore.deleteView();
        if (database != null && name != null)
            database.forgetView(name);
        close();
    }

    /**
     * Creates a new query object for this view. The query can be customized and then executed.
     */
    @InterfaceAudience.Public
    public Query createQuery() {
        return new Query(database, this);
    }

    /**
     * Utility function to use in reduce blocks. Totals an array of Numbers.
     */
    @InterfaceAudience.Public
    public static double totalValues(List<Object> values) {
        double total = 0;
        for (Object object : values) {
            if (object instanceof Number) {
                Number number = (Number) object;
                total += number.doubleValue();
            } else {
                Log.w(Log.TAG_VIEW, "Warning non-numeric value found in totalValues: %s", object);
            }
        }
        return total;
    }

    /**
     * The registered object, if any, that can compile map/reduce functions from source code.
     */
    @InterfaceAudience.Public
    public static ViewCompiler getCompiler() {
        return compiler;
    }

    /**
     * Registers an object that can compile map/reduce functions from source code.
     */
    @InterfaceAudience.Public
    public static void setCompiler(ViewCompiler compiler) {
        View.compiler = compiler;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal (CBLView+Internal.h)
    ///////////////////////////////////////////////////////////////////////////

    @InterfaceAudience.Private
    public int getTotalRows() {
        return viewStore.getTotalRows();
    }

    public void close() {
        if (viewStore != null)
            viewStore.close();
        viewStore = null;
        database = null;
    }

    @InterfaceAudience.Private
    public void setCollation(TDViewCollation collation) {
        viewStore.setCollation(collation);
    }

    /**
     * Updates the view's index (incrementally) if necessary.
     *
     * @return 200 if updated, 304 if already up-to-date, else an error code
     */
    @InterfaceAudience.Private
    public void updateIndex() throws CouchbaseLiteException {
        viewStore.updateIndex();
    }

    /**
     * Queries the view. Does NOT first update the index.
     *
     * @param options The options to use.
     * @return An array of QueryRow objects.
     */
    @InterfaceAudience.Private
    public List<QueryRow> query(QueryOptions options) throws CouchbaseLiteException {
        if (options == null)
            options = new QueryOptions();
        if (groupOrReduce(options))
            return viewStore.reducedQuery(options);
        else
            return viewStore.regularQuery(options);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Public Static Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Changes a maxKey into one that also extends to any key it matches as a prefix
     */
    @InterfaceAudience.Private
    public static Object keyForPrefixMatch(Object key, int depth) {
        if (depth < 1) {
            return key;
        } else if (key instanceof String) {
            // Kludge: prefix match a string by appending max possible character value to it
            return (String) key + "\uffff";
        } else if (key instanceof List) {
            List<Object> nuKey = new ArrayList<Object>(((List<Object>) key));
            if (depth == 1) {
                nuKey.add(new HashMap<String, Object>());
            } else {
                Object lastObject = keyForPrefixMatch(nuKey.get(nuKey.size() - 1), depth - 1);
                nuKey.set(nuKey.size() - 1, lastObject);
            }
            return nuKey;
        } else {
            return key;
        }
    }

    private boolean groupOrReduce(QueryOptions options) {
        if (options.isGroup() || options.getGroupLevel() > 0)
            return true;
        else if (options.isReduceSpecified())
            return options.isReduce();
        else
            return this.reduceBlock != null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // For Debugging
    ///////////////////////////////////////////////////////////////////////////

    @InterfaceAudience.Private
    protected List<Map<String, Object>> dump() {
        return viewStore.dump();
    }
}
