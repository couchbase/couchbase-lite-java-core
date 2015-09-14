//
//  SQLiteViewStore.java
//
//  Created by Hideki Itakura on 6/16/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite.store;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Emitter;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Predicate;
import com.couchbase.lite.QueryOptions;
import com.couchbase.lite.QueryRow;
import com.couchbase.lite.Reducer;
import com.couchbase.lite.Status;
import com.couchbase.lite.TransactionalTask;
import com.couchbase.lite.View;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.ContentValues;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;
import com.couchbase.lite.support.JsonDocument;
import com.couchbase.lite.util.CountDown;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.SQLiteUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLiteViewStore implements ViewStore, QueryRowStore {

    public static String TAG = Log.TAG_VIEW;

    private abstract class AbstractMapEmitBlock implements Emitter {
        protected long sequence = 0;

        void setSequence(long sequence) {
            this.sequence = sequence;
        }
    }

    private static final int REDUCE_BATCH_SIZE = 100;

    // public
    private String name;
    private ViewStoreDelegate delegate;

    // private
    private SQLiteStore store;
    private int viewID;
    private View.TDViewCollation collation;
    private String _mapTableName;

    ///////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////

    protected SQLiteViewStore(SQLiteStore store, String name, boolean create) throws CouchbaseLiteException{
        this.store = store;
        this.name = name;
        this.viewID = -1; // means 'unknown'
        this.collation = View.TDViewCollation.TDViewCollationUnicode;

        if(!create && getViewID() <= 0)
            throw new CouchbaseLiteException(Status.NOT_FOUND);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Implementation of ViewStorage
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ViewStoreDelegate getDelegate() {
        return delegate;
    }

    @Override
    public void setDelegate(ViewStoreDelegate delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setCollation(View.TDViewCollation collation) {
        this.collation = collation;
    }

    @Override
    public void close() {
        store = null;
        viewID = -1;
    }

    @Override
    public void deleteIndex() {
        if (getViewID() <= 0) {
            return;
        }
        String sql = "DROP TABLE IF EXISTS 'maps_#'; " +
                "UPDATE views SET lastSequence=0, total_docs=0 WHERE view_id=#";
        if (!runStatements(sql))
            Log.w(TAG, "Couldn't delete view _index `%s`", name);
    }

    @Override
    public void deleteView() {
        store.runInTransaction(new TransactionalTask() {
            @Override
            public boolean run() {
                deleteIndex();
                String[] whereArgs = {name};
                int rowsAffected = store.getStorageEngine().delete("views", "name=?", whereArgs);
                return rowsAffected > 0 ? true : false;
            }
        });
        viewID = 0;
    }

    /**
     * Updates the version of the view. A change in version means the delegate's map block has
     * changed its semantics, so the _index should be deleted.
     */
    @Override
    public boolean setVersion(String version) {
        // Update the version column in the database. This is a little weird looking because we want
        // to avoid modifying the database if the version didn't change, and because the row might
        // not exist yet.
        SQLiteStorageEngine storage = store.getStorageEngine();
        String sql = "SELECT name, version FROM views WHERE name=?";
        String[] args = {name};
        Cursor cursor = null;
        try {
            cursor = storage.rawQuery(sql, args);
            if (!cursor.moveToNext()) {
                // no such record, so insert
                ContentValues insertValues = new ContentValues();
                insertValues.put("name", name);
                insertValues.put("version", version);
                insertValues.put("total_docs", 0);
                storage.insert("views", null, insertValues);
                createIndex();
                return true; // created new view
            }

            ContentValues updateValues = new ContentValues();
            updateValues.put("version", version);
            updateValues.put("lastSequence", 0);
            updateValues.put("total_docs", 0);
            String[] whereArgs = {name, version};
            int rowsAffected = storage.update("views",
                    updateValues,
                    "name=? AND version!=?",
                    whereArgs);
            return (rowsAffected > 0);
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error setting map block", e);
            return false;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public int getTotalRows() {
        SQLiteStorageEngine storageEngine = store.getStorageEngine();
        String sql = "SELECT total_docs FROM views WHERE name=?";
        String[] args = {name};
        int totalRows = SQLiteUtils.intForQuery(storageEngine, sql, args);
        if (totalRows == -1) { // means unknown
            createIndex();
            totalRows = countTotalRows();
            updateTotalRows(totalRows);
        }
        return totalRows;
    }

    private int countTotalRows() {
        SQLiteStorageEngine storageEngine = store.getStorageEngine();
        String sql = queryString("SELECT COUNT(*) FROM 'maps_#'");
        return SQLiteUtils.intForQuery(storageEngine, sql, null);
    }

    /**
     * The last sequence number that has been indexed.
     */
    @Override
    public long getLastSequenceIndexed() {
        String sql = "SELECT lastSequence FROM views WHERE name=?";
        String[] args = {name};
        Cursor cursor = null;
        long result = -1;
        try {
            cursor = store.getStorageEngine().rawQuery(sql, args);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } catch (Exception e) {
            Log.e(Log.TAG_VIEW, "Error getting last sequence indexed", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    public long getLastSequenceChangedAt() {
        // TODO: Implement
        return 0;
    }

    /**
     * Updates the view's _index (incrementally) if necessary.
     *
     * @return 200 if updated, 304 if already up-to-date, else an error code
     */
    @Override
    @InterfaceAudience.Private
    public void updateIndex() throws CouchbaseLiteException {
        Log.v(Log.TAG_VIEW, "Re-indexing view: %s", name);
        assert (delegate.getMap() != null);

        if (getViewID() <= 0) {
            String msg = String.format("getViewID() < 0");
            throw new CouchbaseLiteException(msg, new Status(Status.NOT_FOUND));
        }

        store.beginTransaction();
        Status result = new Status(Status.INTERNAL_SERVER_ERROR);
        Cursor cursor = null;

        try {
            long last = getLastSequenceIndexed();
            long dbMaxSequence = store.getLastSequence();
            long minLastSequence = dbMaxSequence;

            // First remove obsolete emitted results from the 'maps' table:
            if (last < 0) {
                String msg = String.format("last < 0 (%s)", last);
                throw new CouchbaseLiteException(msg, new Status(Status.INTERNAL_SERVER_ERROR));
            } else if (last < dbMaxSequence) {

                minLastSequence = Math.min(minLastSequence, last);

                if (last == 0) {
                    // If the lastSequence has been reset to 0, make sure to remove any leftover rows:
                    store.getStorageEngine().execSQL(queryString("DELETE FROM 'maps_#'"));
                } else {
                    store.optimizeSQLIndexes();
                    // Delete all obsolete map results (ones from since-replaced revisions):
                    String[] args = {Long.toString(last), Long.toString(last)};
                    store.getStorageEngine().execSQL(
                            queryString("DELETE FROM 'maps_#' WHERE sequence IN ("
                                    + "SELECT parent FROM revs WHERE sequence>? "
                                    + "AND +parent>0 AND +parent<=?)"), args);

                }
            }

            if (minLastSequence == dbMaxSequence) {
                // nothing to do (eg,  kCBLStatusNotModified)
                Log.v(Log.TAG_VIEW, "minLastSequence (%s) == dbMaxSequence (%s), nothing to do",
                        minLastSequence, dbMaxSequence);
                result.setCode(Status.NOT_MODIFIED);
                return;
            }

            // This is the emit() block, which gets called from within the
            // user-defined map() block
            // that's called down below.
            AbstractMapEmitBlock emitBlock = new AbstractMapEmitBlock() {
                @Override
                public void emit(Object key, Object value) {
                    try {
                        String valueJson;
                        String keyJson = Manager.getObjectMapper().writeValueAsString(key);
                        if (value == null) {
                            valueJson = null;
                        } else {
                            valueJson = Manager.getObjectMapper().writeValueAsString(value);
                        }

                        // NOTE: execSQL() is little faster than insert()
                        String[] args = {Long.toString(sequence), keyJson, valueJson};
                        store.getStorageEngine().execSQL(queryString(
                                "INSERT INTO 'maps_#' (sequence, key, value) VALUES(?,?,?)"), args);
                    } catch (Exception e) {
                        Log.e(Log.TAG_VIEW, "Error emitting", e);
                        // find a better way to propagate this back
                    }
                }
            };

            // Now scan every revision added since the last time the view was indexed:

            // NOTE: Below is original Query. In case query result uses a lot of memory,
            //       Android SQLiteDatabase causes null value column. Then it causes the missing
            //       _index data because following logic skip result if column is null.
            //       To avoid the issue, retrieving json field is isolated from original query.
            //       Because json field could be large, maximum size is 2MB.
            // StringBuffer sql = new StringBuffer( "SELECT revs.doc_id, sequence, docid, revid,
            // json, no_attachments, deleted FROM revs, docs WHERE sequence>? AND current!=0 ");

            //TODO: boolean checkDocTypes = docTypes.count > 1 || (allDocTypes && docTypes.count > 0);
            boolean checkDocTypes = false;
            StringBuffer sql = new StringBuffer(
                    "SELECT revs.doc_id, sequence, docid, revid, no_attachments, deleted ");
            if (checkDocTypes)
                sql.append(", doc_type ");
            sql.append("FROM revs, docs WHERE sequence>? AND current!=0 ");
            if (minLastSequence == 0) {
                sql.append("AND deleted=0 ");
            }
            sql.append("AND revs.doc_id = docs.doc_id ORDER BY revs.doc_id, revid DESC");
            String[] selectArgs = {Long.toString(minLastSequence)};
            cursor = store.getStorageEngine().rawQuery(sql.toString(), selectArgs);

            boolean keepGoing = cursor.moveToNext();
            while (keepGoing) {

                // NOTE: skip row if 1st column is null
                // https://github.com/couchbase/couchbase-lite-java-core/issues/497
                if (cursor.isNull(0)) {
                    keepGoing = cursor.moveToNext();
                    continue;
                }

                long docID = cursor.getLong(0);

                // Reconstitute the document as a dictionary:
                long sequence = cursor.getLong(1);
                String docId = cursor.getString(2);
                if (docId.startsWith("_design/")) {  // design docs don't get indexed!
                    keepGoing = cursor.moveToNext();
                    continue;
                }
                String revId = cursor.getString(3);

                boolean noAttachments = cursor.getInt(4) > 0;
                boolean deleted = cursor.getInt(5) > 0;
                String docType = checkDocTypes ? cursor.getString(6) : null;

                while ((keepGoing = cursor.moveToNext()) &&
                        (cursor.isNull(0) || cursor.getLong(0) == docID)) {
                    // Skip rows with the same doc_id -- these are losing conflicts.
                    // NOTE: Or Skip rows if 1st column is null
                    // https://github.com/couchbase/couchbase-lite-java-core/issues/497
                }

                if (minLastSequence > 0) {
                    // Find conflicts with documents from previous indexings.
                    String[] selectArgs2 = {Long.toString(docID), Long.toString(minLastSequence)};

                    Cursor cursor2 = null;
                    try {
                        cursor2 = store.getStorageEngine().rawQuery(
                                "SELECT revid, sequence FROM revs "
                                        + "WHERE doc_id=? AND sequence<=? AND current!=0 AND deleted=0 "
                                        + "ORDER BY revID DESC "
                                        + "LIMIT 1", selectArgs2);

                        if (cursor2.moveToNext()) {
                            String oldRevId = cursor2.getString(0);
                            // This is the revision that used to be the 'winner'.
                            // Remove its emitted rows:
                            long oldSequence = cursor2.getLong(1);
                            String[] args = {Long.toString(oldSequence)};
                            store.getStorageEngine().execSQL(
                                    queryString("DELETE FROM 'maps_#' WHERE sequence=?"), args);
                            if (deleted || RevisionInternal.CBLCompareRevIDs(oldRevId, revId) > 0) {
                                // It still 'wins' the conflict, so it's the one that
                                // should be mapped [again], not the current revision!
                                revId = oldRevId;
                                sequence = oldSequence;
                                deleted = false;
                            }
                        }
                    } finally {
                        if (cursor2 != null) {
                            cursor2.close();
                        }
                    }
                }

                if (deleted) {
                    continue;
                }

                String[] selectArgs3 = {Long.toString(sequence)};
                byte[] json = SQLiteUtils.byteArrayResultForQuery(store.getStorageEngine(),
                        "SELECT json FROM revs WHERE sequence=?", selectArgs3);

                // Get the document properties, to pass to the map function:
                Map<String, Object> properties = store.documentPropertiesFromJSON(
                        json,
                        docId,
                        revId,
                        false,
                        sequence
                );
                if (properties != null) {
                    //TODO checkDocTypes here

                    // Call the user-defined map() to emit new key/value
                    // pairs from this revision:
                    emitBlock.setSequence(sequence);
                    delegate.getMap().map(properties, emitBlock);

                    properties.clear();
                }
            }

            // Finally, record the last revision sequence number that was
            // indexed:
            int newTotalRows = countTotalRows();
            ;
            ContentValues updateValues = new ContentValues();
            updateValues.put("lastSequence", dbMaxSequence);
            updateValues.put("total_docs", newTotalRows);
            String[] whereArgs = {Integer.toString(getViewID())};
            store.getStorageEngine().update("views", updateValues, "view_id=?", whereArgs);

            // FIXME actually count number added :)
            Log.v(Log.TAG_VIEW, "Finished re-indexing view: %s " + " up to sequence %s",
                    name, dbMaxSequence);
            result.setCode(Status.OK);
        } catch (SQLException ex) {
            throw new CouchbaseLiteException(ex, new Status(Status.DB_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (!result.isSuccessful()) {
                Log.w(Log.TAG_VIEW, "Failed to rebuild view %s.  Result code: %d",
                        name, result.getCode());
            }
            if (store != null) {
                store.endTransaction(result.isSuccessful());
            }
        }
    }

    /**
     * Queries the view without performing any reducing or grouping.
     */
    @Override
    public List<QueryRow> regularQuery(final QueryOptions options) throws CouchbaseLiteException {

        final Predicate<QueryRow> postFilter = options.getPostFilter();

        int tmpLimit = QueryOptions.QUERY_OPTIONS_DEFAULT_LIMIT;
        int tmpSkip = 0;
        if (postFilter != null) {
            // #574: Custom post-filter means skip/limit apply to the filtered rows, not to the
            // underlying query, so handle them specially:
            tmpLimit = options.getLimit();
            tmpSkip = options.getSkip();
            if(tmpLimit == 0)
                return new ArrayList<QueryRow>(); // empty result set
            options.setLimit(QueryOptions.QUERY_OPTIONS_DEFAULT_LIMIT);
            options.setSkip(0);
        }

        final CountDown skip = new CountDown(tmpSkip);
        final CountDown limit = new CountDown(tmpLimit);

        final List<QueryRow> rows = new ArrayList<QueryRow>();

        Status status = runQuery(options, new QueryRowBlock() {
            @Override
            public Status onRow(byte[] keyData, byte[] valueData, String docID, Cursor cursor) {
                JsonDocument keyDoc = new JsonDocument(keyData);
                JsonDocument valueDoc = new JsonDocument(valueData);
                long sequence = Long.valueOf(cursor.getString(3));
                RevisionInternal docRevision = null;
                if (options.isIncludeDocs()) {
                    Object valueObject = valueDoc.jsonObject();
                    String linkedID = null;
                    if (valueObject instanceof Map)
                        linkedID = (String) ((Map) valueObject).get("_id");
                    if (linkedID != null) {
                        // Linked document: http://wiki.apache.org/couchdb/Introduction_to_CouchDB_views#Linked_documents
                        String linkedRev = (String) ((Map) valueObject).get("_rev");
                        docRevision = store.getDocument(linkedID, linkedRev, true);
                        sequence = docRevision.getSequence();
                    } else {
                        String revID = cursor.getString(4);
                        byte[] json = cursor.getBlob(5);
                        Map<String, Object> properties = store.documentPropertiesFromJSON(
                                json,
                                docID,
                                revID,
                                false,
                                sequence);
                        docRevision = store.revision(
                                docID,    // docID
                                revID,    // revID
                                false,    // deleted
                                sequence, // sequence
                                properties// properties
                        );
                    }
                }
                QueryRow row = new QueryRow(docID, sequence,
                        keyDoc.jsonObject(), valueDoc.jsonObject(),
                        docRevision, SQLiteViewStore.this);
                if (postFilter != null) {
                    if (!postFilter.apply(row)) {
                        return new Status(Status.OK);
                    }
                    if(skip.getCount() > 0){
                        skip.countDown();
                        return new Status(Status.OK);
                    }

                }
                rows.add(row);

                if(limit.countDown() == 0)
                    return new Status(0); /// stops the iteration
                return new Status(Status.OK);
            }
        });

        // If given keys, sort the output into that order, and add entries for missing keys:
        if(options.getKeys() != null && options.getKeys().size() > 0){
            // Group rows by key:
            Map<Object, List<QueryRow>> rowsByKey = new HashMap<Object, List<QueryRow>>();
            for(QueryRow row:rows){
                List<QueryRow> rs = rowsByKey.get(row.getKey());
                if(rs == null) {
                    rs = new ArrayList<QueryRow>();
                    rowsByKey.put(row.getKey(), rs);
                }
                rs.add(row);
            }

            // Now concatenate them in the order the keys are given in options:
            final List<QueryRow> sortedRows = new ArrayList<QueryRow>();
            for(Object key : options.getKeys()){
                JsonDocument jsonDoc = null;
                try {
                    byte[] keyBytes = Manager.getObjectMapper().writeValueAsBytes(key);
                    jsonDoc = new JsonDocument(keyBytes);
                } catch (JsonProcessingException e) {
                    throw new  CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                }
                List<QueryRow> rs = rowsByKey.get(jsonDoc.jsonObject());
                if(rs != null)
                    sortedRows.addAll(rs);
            }

            return sortedRows;
        }

        return rows;
    }

    /**
     * Queries the view, with reducing or grouping as per the options.
     * in CBL_SQLiteViewStorage.m
     * - (CBLQueryIteratorBlock) reducedQueryWithOptions: (CBLQueryOptions*)options status: (CBLStatus*)outStatus
     */
    @Override
    public List<QueryRow> reducedQuery(QueryOptions options) throws CouchbaseLiteException {
        final Predicate<QueryRow> postFilter = options.getPostFilter();

        final int groupLevel = options.getGroupLevel();
        final boolean group = options.isGroup() || (groupLevel > 0);
        final Reducer reduce = delegate.getReduce();
        if (options.isReduceSpecified()) {
            if (options.isReduce() && reduce == null) {
                Log.w(TAG, String.format(
                        "Cannot use reduce option in view %s which has no reduce block defined",
                        name));
                throw new CouchbaseLiteException(new Status(Status.BAD_PARAM));
            }
        }

        final List<Object> keysToReduce = new ArrayList<Object>(REDUCE_BATCH_SIZE);
        final List<Object> valuesToReduce = new ArrayList<Object>(REDUCE_BATCH_SIZE);
        final Object[] lastKeys = new Object[1];
        lastKeys[0] = null;
        final SQLiteViewStore that = this;

        final List<QueryRow> rows = new ArrayList<QueryRow>();

        Status status = runQuery(options, new QueryRowBlock() {
            @Override
            public Status onRow(byte[] keyData, byte[] valueData, String docID, Cursor cursor) {
                JsonDocument keyDoc = new JsonDocument(keyData);
                JsonDocument valueDoc = new JsonDocument(valueData);
                assert (keyDoc != null);
                Object keyObject = keyDoc.jsonObject();
                if (group && !groupTogether(keyObject, lastKeys[0], groupLevel)) {
                    if (lastKeys[0] != null) {
                        // This pair starts a new group, so reduce & record the last one:
                        Object key = groupKey(lastKeys[0], groupLevel);
                        Object reduced = (reduce != null) ?
                                reduce.reduce(keysToReduce, valuesToReduce, false) :
                                null;
                        QueryRow row = new QueryRow(null, 0, key, reduced, null, that);
                        if (postFilter == null || postFilter.apply(row))
                            rows.add(row);
                        keysToReduce.clear();
                        valuesToReduce.clear();
                    }
                    lastKeys[0] = keyObject;
                }
                //Log.v(TAG, String.format("Query %s: Will reduce row with key=%s, value=%s",
                //        name, new String(keyData), new String(valueData)));

                /*
                Object valueOrData = valueData;
                if(reduce != null && rowValueIsEntireDoc(valueData)){
                    // map fn emitted 'doc' as value, which was stored as a "*" placeholder; expand now:
                    long sequence = Long.valueOf(cursor.getString(3));
                    RevisionInternal rev = store.getDocument(docID, sequence);
                    if(rev == null)
                        Log.w(TAG, String.format("%s: Couldn't load doc for row value", this));
                    else
                        valueOrData = rev.getProperties();
                }
                */
                keysToReduce.add(keyObject);
                valuesToReduce.add(valueDoc.jsonObject());
                return new Status(Status.OK);
            }
        });

        if (keysToReduce != null && keysToReduce.size() > 0) {
            // Finish the last group (or the entire list, if no grouping):
            Object key = group ? groupKey(lastKeys[0], groupLevel) : null;
            Object reduced = (reduce != null) ?
                    reduce.reduce(keysToReduce, valuesToReduce, false) :
                    null;
            Log.v(TAG, String.format("Query %s: Reduced to key=%s, value=%s",
                    name, key, reduced));
            QueryRow row = new QueryRow(null, 0, key, reduced, null, that);
            if (postFilter == null || postFilter.apply(row)) {
                rows.add(row);
            }
        }
        return rows;
    }

    @Override
    public List<Map<String, Object>> dump() {
        if (getViewID() < 0)
            return null;
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        String sql = "SELECT sequence, key, value FROM 'maps_#' ORDER BY key";
        Cursor cursor = null;
        try {
            cursor = store.getStorageEngine().rawQuery(queryString(sql), null);
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                Map<String, Object> row = new HashMap<String, Object>();
                row.put("seq", cursor.getInt(0));
                row.put("key", cursor.getString(1));
                row.put("value", cursor.getString(2));
                result.add(row);
                cursor.moveToNext();
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }
        return result;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Implementation of QueryRowStorage
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public boolean rowValueIsEntireDoc(byte[] valueData) {
        return valueData.length == 1 && (new String(valueData)).equals("*");
    }

    @Override
    public Object parseRowValue(byte[] valueData) {
        // TODO: Implement
        return null;
    }

    @Override
    public Map<String, Object> getDocumentProperties(String docID, long sequence) {
        // TODO: Implement
        return null;
    }


    ///////////////////////////////////////////////////////////////////////////
    // Internal (Private) Instance Methods
    ///////////////////////////////////////////////////////////////////////////

    private int getViewID() {
        if (viewID < 0) {
            String sql = "SELECT view_id FROM views WHERE name=?";
            String[] args = {name};
            Cursor cursor = null;
            try {
                cursor = store.getStorageEngine().rawQuery(sql, args);
                if (cursor.moveToNext()) {
                    viewID = cursor.getInt(0);
                }
            } catch (SQLException e) {
                Log.e(Log.TAG_VIEW, "Error getting view id", e);
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }
        return viewID;
    }

    // pragma mark - QUERYING:

    /**
     * Generates and runs the SQL SELECT statement for a view query, calling the onRow callback.
     * in CBL_SQLiteViewStorage.m
     * - (CBLStatus) _runQueryWithOptions: (const CBLQueryOptions*)options onRow: (QueryRowBlock)onRow
     */
    private Status runQuery(QueryOptions options, QueryRowBlock block) {
        if (options == null)
            options = new QueryOptions();

        // OPT: It would be faster to use separate tables for raw-or ascii-collated views so that
        // they could be indexed with the right collation, instead of having to specify it here.
        String collationStr = "";
        if (collation == View.TDViewCollation.TDViewCollationASCII)
            collationStr += " COLLATE JSON_ASCII";
        else if (collation == View.TDViewCollation.TDViewCollationRaw)
            collationStr += " COLLATE JSON_RAW";


        StringBuffer sql = new StringBuffer("SELECT key, value, docid, revs.sequence");
        if (options.isIncludeDocs()) {
            sql.append(", revid, json");
        }
        sql.append(String.format(" FROM 'maps_%s', revs, docs", mapTableName()));
        sql.append(" WHERE 1");

        List<String> argsList = new ArrayList<String>();

        if (options.getKeys() != null) {
            sql.append(" AND key in (");
            String item = "?";
            for (Object key : options.getKeys()) {
                sql.append(item);
                item = ", ?";
                argsList.add(toJSONString(key));
            }
            sql.append(")");
        }

        Object minKey = options.getStartKey();
        Object maxKey = options.getEndKey();
        String minKeyDocId = options.getStartKeyDocId();
        String maxKeyDocId = options.getEndKeyDocId();

        boolean inclusiveMin = options.isInclusiveStart();
        boolean inclusiveMax = options.isInclusiveEnd();
        if (options.isDescending()) {
            Object min = minKey;
            minKey = maxKey;
            maxKey = min;
            inclusiveMin = inclusiveMax;
            inclusiveMax = true;
            minKeyDocId = options.getEndKeyDocId();
            maxKeyDocId = options.getStartKeyDocId();
        }

        if (minKey != null) {
            String minKeyJSON = toJSONString(minKey);
            sql.append(inclusiveMin ? " AND key >= ?" : " AND key > ?");
            sql.append(collationStr);
            argsList.add(minKeyJSON);
            if (minKeyDocId != null && inclusiveMin) {
                //OPT: This calls the JSON collator a 2nd time unnecessarily.
                sql.append(String.format(" AND (key > ? %s OR docid >= ?)", collationStr));
                argsList.add(minKeyJSON);
                argsList.add(minKeyDocId);
            }
        }

        if (maxKey != null) {
            maxKey = View.keyForPrefixMatch(maxKey, options.getPrefixMatchLevel());
            String maxKeyJSON = toJSONString(maxKey);
            sql.append(inclusiveMax ? " AND key <= ?" : " AND key < ?");
            sql.append(collationStr);
            argsList.add(maxKeyJSON);
            if (maxKeyDocId != null && inclusiveMax) {
                sql.append(String.format(" AND (key < ? %s OR docid <= ?)", collationStr));
                argsList.add(maxKeyJSON);
                argsList.add(maxKeyDocId);
            }
        }

        sql.append(String.format(
                " AND revs.sequence = 'maps_%s'.sequence AND docs.doc_id = revs.doc_id ORDER BY key",
                mapTableName()));
        sql.append(collationStr);
        if (options.isDescending()) {
            sql.append(" DESC");
        }
        sql.append(options.isDescending() ? ", docid DESC" : ", docid");

        sql.append(" LIMIT ? OFFSET ?");
        argsList.add(Integer.toString(options.getLimit()));
        argsList.add(Integer.toString(options.getSkip()));

        Log.v(Log.TAG_VIEW, "Query %s: %s | args: %s", name, sql.toString(), argsList);

        Status status = new Status(Status.OK);
        Cursor cursor = null;
        try {
            cursor = store.getStorageEngine().rawQuery(sql.toString(),
                    argsList.toArray(new String[argsList.size()]));
            // regular query
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                // Call the block!
                byte[] keyData = cursor.getBlob(0);
                byte[] valueData = cursor.getBlob(1);
                String docID = cursor.getString(2);
                status = block.onRow(keyData, valueData, docID, cursor);
                if (status.isError())
                    break;
                else if (status.getCode() <= 0) {
                    status = new Status(Status.OK);
                    break;
                }
                cursor.moveToNext();
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }
        return status;
    }

    private String toJSONString(Object object) {
        if (object == null) {
            return null;
        }
        String result = null;
        try {
            result = Manager.getObjectMapper().writeValueAsString(object);
        } catch (Exception e) {
            Log.w(Log.TAG_VIEW, "Exception serializing object to json: %s", e, object);
        }
        return result;
    }

    private void updateTotalRows(int totalRows) {
        ContentValues values = new ContentValues();
        values.put("total_docs=", totalRows);
        store.getStorageEngine().update("views", values, "view_id=?",
                new String[]{String.valueOf(getViewID())});
    }

    private String mapTableName() {
        if (_mapTableName == null) {
            _mapTableName = String.format("%d", getViewID());
        }
        return _mapTableName;
    }

    /**
     * The name of the map table is dynamic, based on the ID of the view. This method replaces a '#'
     * with the view ID in a query string.
     */
    private String queryString(String sql) {
        return sql.replaceAll("#", mapTableName());
    }

    private boolean runStatements(final String sql) {
        final SQLiteStore db = store;
        return db.runInTransaction(new TransactionalTask() {
            @Override
            public boolean run() {
                if (!db.runStatements(queryString(sql))) {
                    return false;
                }
                return true;
            }
        });
    }

    private void createIndex() {
        String sql = "CREATE TABLE IF NOT EXISTS 'maps_#' (" +
                "sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE," +
                "key TEXT NOT NULL COLLATE JSON," +
                "value TEXT)";
        if (!runStatements(sql))
            Log.w(TAG, "Couldn't create view _index `%s`", name);
    }


    ///////////////////////////////////////////////////////////////////////////
    // Internal (Private) Static Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Are key1 and key2 grouped together at this groupLevel?
     */
    private static boolean groupTogether(Object key1, Object key2, int groupLevel) {
        if (groupLevel == 0 || !(key1 instanceof List) || !(key2 instanceof List)) {
            return key1.equals(key2);
        }
        @SuppressWarnings("unchecked")
        List<Object> key1List = (List<Object>) key1;
        @SuppressWarnings("unchecked")
        List<Object> key2List = (List<Object>) key2;

        // if either key list is smaller than groupLevel and the key lists are different
        // sizes, they cannot be equal.
        if ((key1List.size() < groupLevel || key2List.size() < groupLevel) &&
                key1List.size() != key2List.size()) {
            return false;
        }

        int end = Math.min(groupLevel, Math.min(key1List.size(), key2List.size()));
        for (int i = 0; i < end; ++i) {
            if (!key1List.get(i).equals(key2List.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the prefix of the key to use in the result row, at this groupLevel
     */
    public static Object groupKey(Object key, int groupLevel) {
        if (groupLevel > 0 && (key instanceof List) && (((List<Object>) key).size() > groupLevel)) {
            return ((List<Object>) key).subList(0, groupLevel);
        } else {
            return key;
        }
    }
}
