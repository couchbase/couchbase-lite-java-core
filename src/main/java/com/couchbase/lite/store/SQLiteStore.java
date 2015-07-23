//
//  SQLiteStore.java
//
//  Created by Hideki Itakura on 6/16/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite.store;

import com.couchbase.lite.BlobKey;
import com.couchbase.lite.ChangesOptions;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.DocumentChange;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Misc;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryOptions;
import com.couchbase.lite.QueryRow;
import com.couchbase.lite.ReplicationFilter;
import com.couchbase.lite.Revision;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.TransactionalTask;
import com.couchbase.lite.View;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.ContentValues;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;
import com.couchbase.lite.storage.SQLiteStorageEngineFactory;
import com.couchbase.lite.support.RevisionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.SQLiteUtils;
import com.couchbase.lite.util.TextUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQLiteStore implements Store {
    public String TAG = Log.TAG_DATABASE;

    public static String kDBFilename = "db.sqlite3";

    // Default value for maxRevTreeDepth, the max rev depth to preserve in a prune operation
    private static final int DEFAULT_MAX_REVS = Integer.MAX_VALUE;

    // First-time initialization:
    // (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
    // monotonically increasing, never reused. See <http://www.sqlite.org/autoinc.html>)
    public static final String SCHEMA = "" +
            // docs
            "CREATE TABLE docs ( " +
            "        doc_id INTEGER PRIMARY KEY, " +
            "        docid TEXT UNIQUE NOT NULL); " +
            "    CREATE INDEX docs_docid ON docs(docid); " +
            // revs
            "    CREATE TABLE revs ( " +
            "        sequence INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "        doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE, " +
            "        revid TEXT NOT NULL COLLATE REVID, " +
            "        parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL, " +
            "        current BOOLEAN, " +
            "        deleted BOOLEAN DEFAULT 0, " +
            "        json BLOB, " +
            "        no_attachments BOOLEAN, " +
            "        UNIQUE (doc_id, revid)); " +
            "    CREATE INDEX revs_parent ON revs(parent); " +
            "    CREATE INDEX revs_by_docid_revid ON revs(doc_id, revid desc, current, deleted); " +
            "    CREATE INDEX revs_current ON revs(doc_id, current desc, deleted, revid desc); " +
            // localdocs
            "    CREATE TABLE localdocs ( " +
            "        docid TEXT UNIQUE NOT NULL, " +
            "        revid TEXT NOT NULL COLLATE REVID, " +
            "        json BLOB); " +
            "    CREATE INDEX localdocs_by_docid ON localdocs(docid); " +
            // views
            "    CREATE TABLE views ( " +
            "        view_id INTEGER PRIMARY KEY, " +
            "        name TEXT UNIQUE NOT NULL," +
            "        version TEXT, " +
            "        lastsequence INTEGER DEFAULT 0," +
            "        total_docs INTEGER DEFAULT -1); " +
            "    CREATE INDEX views_by_name ON views(name); " +
            // info
            "    CREATE TABLE info (" +
            "        key TEXT PRIMARY KEY," +
            "        value TEXT);" +
            // version
            "    PRAGMA user_version = 17"; // at the end, update user_version
    //OPT: Would be nice to use partial indexes but that requires SQLite 3.8 and makes the
    // db file only readable by SQLite 3.8+, i.e. the file would not be portable to iOS 8
    // which only has SQLite 3.7 :(
    // On the revs_parent index we could add "WHERE parent not null".

    // transactionLevel is per thread
    static class TransactionLevel extends ThreadLocal<Integer> {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    }

    private String directory;
    private String path;
    private Manager manager;
    private SQLiteStorageEngine storageEngine;
    private TransactionLevel transactionLevel;
    private StoreDelegate delegate;
    private int maxRevTreeDepth;

    ///////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////

    public SQLiteStore(String directory, Manager manager, StoreDelegate delegate) {
        assert (new File(directory).isAbsolute()); // path must be absolute
        this.directory = directory;
        File dir = new File(directory);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("directory '" + directory + "' does not exist or not directory");
        }
        this.path = new File(directory, kDBFilename).getPath();
        this.manager = manager;
        this.storageEngine = null;
        this.transactionLevel = new TransactionLevel();
        this.delegate = delegate;
        this.maxRevTreeDepth = DEFAULT_MAX_REVS;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Implementation of Storage
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZATION AND CONFIGURATION:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public boolean databaseExists(String directory) {
        return new File(directory, kDBFilename).exists();
    }

    @Override
    public synchronized boolean open() {

        // Create the storage engine.
        SQLiteStorageEngineFactory sqliteStorageEngineFactoryDefault =
                manager.getContext().getSQLiteStorageEngineFactory();
        storageEngine = sqliteStorageEngineFactoryDefault.createStorageEngine();

        // Try to open the storage engine and stop if we fail.
        if (storageEngine == null || !storageEngine.open(path)) {
            String msg = "Unable to create a storage engine, fatal error";
            Log.e(TAG, msg);
            throw new IllegalStateException(msg);
        }

        // Stuff we need to initialize every time the sqliteDb opens:
        if (!initialize("PRAGMA foreign_keys = ON;")) {
            Log.e(TAG, "Error turning on foreign keys");
            return false;
        }

        // Check the user_version number we last stored in the sqliteDb:
        int dbVersion = storageEngine.getVersion();

        // Incompatible version changes increment the hundreds' place:
        if (dbVersion >= 200) {
            Log.e(TAG, "Database: Database version (%d) is newer than I know how to work with",
                    dbVersion);
            storageEngine.close();
            return false;
        }

        boolean isNew = (dbVersion == 0);
        boolean isSuccessful = false;

        // BEGIN TRANSACTION
        if (!beginTransaction()) {
            storageEngine.close();
            return false;
        }

        try {
            if (dbVersion < 17) {
                // First-time initialization:
                // (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
                // monotonically increasing, never reused. See <http://www.sqlite.org/autoinc.html>)

                if (!isNew) {
                    Log.w(TAG, "CBLDatabase: Database version (%d) is older than I know how to work with",
                            dbVersion);
                    storageEngine.close();
                    return false;
                }

                if (!initialize(SCHEMA)) {
                    return false;
                }
                dbVersion = 17;
            }


            if (dbVersion < 21) {
                // Version 18:
                String upgradeSql = "ALTER TABLE revs ADD COLUMN doc_type TEXT; " +
                        "PRAGMA user_version = 21";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 21;
            }

            if (dbVersion < 101) {
                String upgradeSql = "PRAGMA user_version = 101";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 101;
            }

            if (isNew) {
                optimizeSQLIndexes(); // runs ANALYZE query
            }

            // successfully updated storageEngine schema
            isSuccessful = true;

        } finally {
            // END TRANSACTION WITH COMMIT OR ROLLBACK
            endTransaction(isSuccessful);

            // if failed, close storageEngine before return
            if (!isSuccessful) {
                storageEngine.close();
            }
        }

        return true;
    }

    @Override
    public void close() {
        if (storageEngine != null && storageEngine.isOpen())
            storageEngine.close();
        storageEngine = null;
    }

    @Override
    public void setDelegate(StoreDelegate delegate) {
        this.delegate = delegate;
    }

    @Override
    public StoreDelegate getDelegate() {
        return delegate;
    }

    /**
     * Set the maximum depth of a document's revision tree (or, max length of its revision history.)
     * Revisions older than this limit will be deleted during a -compact: operation.
     * Smaller values save space, at the expense of making document conflicts somewhat more likely.
     */
    @Override
    public void setMaxRevTreeDepth(int maxRevTreeDepth) {
        this.maxRevTreeDepth = maxRevTreeDepth;
    }

    /**
     * Get the maximum depth of a document's revision tree (or, max length of its revision history.)
     * Revisions older than this limit will be deleted during a -compact: operation.
     * Smaller values save space, at the expense of making document conflicts somewhat more likely.
     */
    @Override
    public int getMaxRevTreeDepth() {
        return maxRevTreeDepth;
    }

    ///////////////////////////////////////////////////////////////////////////
    // DATABASE ATTRIBUTES & OPERATIONS:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public long setInfo(String key, String info) {
        ContentValues args = new ContentValues();
        args.put("key", key);
        args.put("value", info);
        return storageEngine.insertWithOnConflict("info", null, args,
                SQLiteStorageEngine.CONFLICT_REPLACE);
    }

    @Override
    public String getInfo(String key) {
        String result = null;
        Cursor cursor = null;
        try {
            String[] args = {key};
            cursor = storageEngine.rawQuery("SELECT value FROM info WHERE key=?", args);
            if (cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error querying " + key, e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    public int getDocumentCount() {
        String sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
        Cursor cursor = null;
        int result = 0;
        try {
            cursor = storageEngine.rawQuery(sql, null);
            if (cursor.moveToNext()) {
                result = cursor.getInt(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting document count", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * The latest sequence number used.  Every new revision is assigned a new sequence number,
     * so this property increases monotonically as changes are made to the storageEngine. It can be
     * used to check whether the storageEngine has changed between two points in time.
     */
    public long getLastSequence() {
        String sql = "SELECT MAX(sequence) FROM revs";
        Cursor cursor = null;
        long result = 0;
        try {
            cursor = storageEngine.rawQuery(sql, null);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting last sequence", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * Is a transaction active?
     */
    @Override
    public boolean inTransaction() {
        return transactionLevel.get() > 0;
    }

    @Override
    public void compact() throws CouchbaseLiteException {
        // Start off by pruning each revision tree's depth:
        pruneRevsToMaxDepth(maxRevTreeDepth);

        // Remove the JSON of non-current revisions, which is most of the space.
        try {
            Log.v(TAG, "Deleting JSON of old revisions...");
            ContentValues args = new ContentValues();
            args.put("json", (String) null);
            args.put("doc_type", (String) null);
            args.put("no_attachments", 1);
            int changes = storageEngine.update("revs", args, "current=0", null);
            Log.v(TAG, "... deleted %d revisions", changes);
        } catch (SQLException e) {
            Log.e(TAG, "Error compacting", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }

        Log.v(TAG, "Vacuuming SQLite database...");
        try {
            storageEngine.execSQL("VACUUM");
        } catch (SQLException e) {
            Log.e(TAG, "Error vacuuming sqliteDb", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }

        Log.v(TAG, "...Finished database compaction.");
    }

    @Override
    public boolean runInTransaction(TransactionalTask transactionalTask) {

        boolean shouldCommit = true;

        beginTransaction();
        try {
            shouldCommit = transactionalTask.run();
        } catch (Exception e) {
            shouldCommit = false;
            Log.e(TAG, e.toString(), e);
            throw new RuntimeException(e);
        } finally {
            endTransaction(shouldCommit);
        }

        return shouldCommit;
    }

    ///////////////////////////////////////////////////////////////////////////
    // DOCUMENTS:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public RevisionInternal getDocument(String docID, String revID, boolean withBody) {

        long docNumericID = getDocNumericID(docID);
        if (docNumericID < 0) {
            return null;
        }

        RevisionInternal result = null;
        String sql;

        Cursor cursor = null;
        try {
            cursor = null;
            String cols = "revid, deleted, sequence";
            if (withBody) {
                cols += ", json";
            }
            if (revID != null) {
                sql = "SELECT " + cols + " FROM revs WHERE revs.doc_id=? AND revid=? AND json notnull LIMIT 1";
                String[] args = {Long.toString(docNumericID), revID};
                cursor = storageEngine.rawQuery(sql, args);
            } else {
                sql = "SELECT " + cols + " FROM revs WHERE revs.doc_id=? and current=1 and deleted=0 ORDER BY revid DESC LIMIT 1";
                String[] args = {Long.toString(docNumericID)};
                cursor = storageEngine.rawQuery(sql, args);
            }

            if (cursor.moveToNext()) {
                if (revID == null) {
                    revID = cursor.getString(0);
                }
                boolean deleted = (cursor.getInt(1) > 0);
                result = new RevisionInternal(docID, revID, deleted);
                result.setSequence(cursor.getLong(2));
                if (withBody) {
                    byte[] json = cursor.getBlob(3);
                    result.setJSON(json);
                }
            } else {
                // revID != null?Status.NOT_FOUND:Status.DELTED
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting document with id and rev", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    public RevisionInternal loadRevisionBody(RevisionInternal rev)
            throws CouchbaseLiteException {
        if (rev.getBody() != null && rev.getSequence() != 0) // no-op
            return rev;

        assert (rev.getDocID() != null && rev.getRevID() != null);

        long docNumericID = getDocNumericID(rev.getDocID());
        if (docNumericID <= 0)
            throw new CouchbaseLiteException(Status.NOT_FOUND);

        Cursor cursor = null;
        Status result = new Status(Status.NOT_FOUND);
        try {
            String sql = "SELECT sequence, json FROM revs WHERE doc_id=? AND revid=? LIMIT 1";
            String[] args = {String.valueOf(docNumericID), rev.getRevID()};
            cursor = storageEngine.rawQuery(sql, args);
            if (cursor.moveToNext()) {
                byte[] json = cursor.getBlob(1);
                if (json != null) {
                    result.setCode(Status.OK);
                    rev.setSequence(cursor.getLong(0));
                    rev.setJSON(json);
                }
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error loading revision body", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        if (result.getCode() == Status.NOT_FOUND) {
            throw new CouchbaseLiteException(result);
        }

        return rev;
    }

    @Override
    public RevisionInternal getParentRevision(RevisionInternal rev) {

        // First get the parent's sequence:
        long seq = rev.getSequence();
        if (seq > 0) {
            seq = SQLiteUtils.longForQuery(storageEngine,
                    "SELECT parent FROM revs WHERE sequence=?",
                    new String[]{Long.toString(seq)});
        } else {
            long docNumericID = getDocNumericID(rev.getDocID());
            if (docNumericID <= 0) {
                return null;
            }
            String[] args = new String[]{Long.toString(docNumericID), rev.getRevID()};
            seq = SQLiteUtils.longForQuery(storageEngine,
                    "SELECT parent FROM revs WHERE doc_id=? and revid=?", args);
        }

        if (seq == 0) {
            return null;
        }

        // Now get its revID and deletion status:
        RevisionInternal result = null;

        String[] args = {Long.toString(seq)};
        String queryString = "SELECT revid, deleted FROM revs WHERE sequence=?";
        Cursor cursor = null;

        try {
            cursor = storageEngine.rawQuery(queryString, args);
            if (cursor.moveToNext()) {
                String revId = cursor.getString(0);
                boolean deleted = (cursor.getInt(1) > 0);
                result = new RevisionInternal(rev.getDocID(), revId, deleted/*, this*/);
                result.setSequence(seq);
            }
        } finally {
            cursor.close();
        }
        return result;
    }

    /**
     * Returns an array of TDRevs in reverse chronological order, starting with the given revision.
     */
    @Override
    public List<RevisionInternal> getRevisionHistory(RevisionInternal rev) {
        String docId = rev.getDocID();
        String revId = rev.getRevID();
        assert ((docId != null) && (revId != null));

        long docNumericId = getDocNumericID(docId);
        if (docNumericId < 0) {
            return null;
        } else if (docNumericId == 0) {
            return new ArrayList<RevisionInternal>();
        }

        String sql = "SELECT sequence, parent, revid, deleted, json isnull FROM revs " +
                "WHERE doc_id=? ORDER BY sequence DESC";
        String[] args = {Long.toString(docNumericId)};
        Cursor cursor = null;

        List<RevisionInternal> result;
        try {
            cursor = storageEngine.rawQuery(sql, args);

            cursor.moveToNext();
            long lastSequence = 0;
            result = new ArrayList<RevisionInternal>();
            while (!cursor.isAfterLast()) {
                long sequence = cursor.getLong(0);
                boolean matches = false;
                if (lastSequence == 0) {
                    matches = revId.equals(cursor.getString(2));
                } else {
                    matches = (sequence == lastSequence);
                }
                if (matches) {
                    revId = cursor.getString(2);
                    boolean deleted = (cursor.getInt(3) > 0);
                    boolean missing = (cursor.getInt(4) > 0);
                    RevisionInternal aRev = new RevisionInternal(docId, revId, deleted);
                    aRev.setMissing(missing);
                    aRev.setSequence(cursor.getLong(0));
                    result.add(aRev);
                    lastSequence = cursor.getLong(1);
                    if (lastSequence == 0) {
                        break;
                    }
                }
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting revision history", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    private RevisionList getAllRevisions(String docId, long docNumericID, boolean onlyCurrent) {
        String sql = null;
        if (onlyCurrent)
            sql = "SELECT sequence, revid, deleted FROM revs " +
                    "WHERE doc_id=? AND current ORDER BY sequence DESC";
        else
            sql = "SELECT sequence, revid, deleted FROM revs " +
                    "WHERE doc_id=? ORDER BY sequence DESC";
        String[] args = {Long.toString(docNumericID)};
        Cursor cursor = storageEngine.rawQuery(sql, args);
        RevisionList result = null;
        try {
            cursor.moveToNext();
            result = new RevisionList();
            while (!cursor.isAfterLast()) {
                RevisionInternal rev = new RevisionInternal(docId,
                        cursor.getString(1),
                        (cursor.getInt(2) > 0));
                rev.setSequence(cursor.getLong(0));
                result.add(rev);
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            return null;
        } finally {
            if (cursor != null)
                cursor.close();
        }
        return result;
    }

    @Override
    public List<String> getPossibleAncestorRevisionIDs(RevisionInternal rev, int limit,
                                                       AtomicBoolean onlyAttachments) {
        int generation = rev.getGeneration();
        if (generation <= 1)
            return null;

        long docNumericID = getDocNumericID(rev.getDocID());
        if (docNumericID <= 0)
            return null;

        List<String> revIDs = new ArrayList<String>();

        int sqlLimit = limit > 0 ? (int) limit : -1;     // SQL uses -1, not 0, to denote 'no limit'
        String sql = "SELECT revid, sequence FROM revs WHERE doc_id=? and revid < ?" +
                " and deleted=0 and json not null" +
                " ORDER BY sequence DESC LIMIT ?";
        String[] args = {Long.toString(docNumericID), generation + "-", Integer.toString(sqlLimit)};

        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(sql, args);
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                if (onlyAttachments != null && revIDs.size() == 0) {
                    onlyAttachments.set(sequenceHasAttachments(cursor.getLong(1)));
                }
                revIDs.add(cursor.getString(0));
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting all revisions of document", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return revIDs;
    }

    @Override
    public String findCommonAncestor(RevisionInternal rev, List<String> revIDs) {
        String result = null;

        if (revIDs.size() == 0)
            return null;
        String docId = rev.getDocID();
        long docNumericID = getDocNumericID(docId);
        if (docNumericID <= 0)
            return null;
        String quotedRevIds = TextUtils.joinQuoted(revIDs);
        String sql = "SELECT revid FROM revs " +
                "WHERE doc_id=? and revid in (" + quotedRevIds + ") and revid <= ? " +
                "ORDER BY revid DESC LIMIT 1";
        String[] args = {Long.toString(docNumericID)};

        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(sql, args);
            cursor.moveToNext();
            if (!cursor.isAfterLast()) {
                result = cursor.getString(0);
            }

        } catch (SQLException e) {
            Log.e(TAG, "Error getting all revisions of document", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    @Override
    public int findMissingRevisions(RevisionList touchRevs) throws SQLException {
        int numRevisionsRemoved = 0;
        if (touchRevs.size() == 0) {
            return numRevisionsRemoved;
        }

        String quotedDocIds = TextUtils.joinQuoted(touchRevs.getAllDocIds());
        String quotedRevIds = TextUtils.joinQuoted(touchRevs.getAllRevIds());

        String sql = "SELECT docid, revid FROM revs, docs " +
                "WHERE docid IN (" +
                quotedDocIds +
                ") AND revid in (" +
                quotedRevIds + ")" +
                " AND revs.doc_id == docs.doc_id";

        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(sql, null);
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                RevisionInternal rev = touchRevs.revWithDocIdAndRevId(cursor.getString(0),
                        cursor.getString(1));

                if (rev != null) {
                    touchRevs.remove(rev);
                    numRevisionsRemoved += 1;
                }

                cursor.moveToNext();
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return numRevisionsRemoved;
    }

    /**
     * - (NSSet*) findAllAttachmentKeys: (NSError**)outError
     */
    @Override
    public Set<BlobKey> findAllAttachmentKeys() throws CouchbaseLiteException {
        Set<BlobKey> allKeys = new HashSet<BlobKey>();
        String sql = "SELECT json FROM revs WHERE no_attachments != 1";
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(sql, null);
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                byte[] json = cursor.getBlob(0);
                if (json != null && json.length > 0) {
                    try {
                        Map<String, Object> docProperties = Manager.getObjectMapper().readValue(json, Map.class);
                        if (docProperties.containsKey("_attachments")) {
                            Map<String, Object> attachments = (Map<String, Object>) docProperties.get("_attachments");
                            Iterator<String> itr = attachments.keySet().iterator();
                            while (itr.hasNext()) {
                                String name = itr.next();
                                Map<String, Object> attachment = (Map<String, Object>) attachments.get(name);
                                String digest = (String) attachment.get("digest");
                                BlobKey key = new BlobKey(digest);
                                allKeys.add(key);
                            }
                        }
                    } catch (IOException e) {
                        Log.e(TAG, e.toString(), e);
                    }
                }
                cursor.moveToNext();
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return allKeys;
    }

    /**
     * - (CBLQueryIteratorBlock) getAllDocs: (CBLQueryOptions*)options
     * status: (CBLStatus*)outStatus
     */
    @Override
    public Map<String, Object> getAllDocs(QueryOptions options) throws CouchbaseLiteException {

        Map<String, Object> result = new HashMap<String, Object>();
        List<QueryRow> rows = new ArrayList<QueryRow>();
        if (options == null) {
            options = new QueryOptions();
        }
        boolean includeDeletedDocs = (options.getAllDocsMode() == Query.AllDocsMode.INCLUDE_DELETED);

        long updateSeq = 0;
        if (options.isUpdateSeq()) {
            updateSeq = getLastSequence();  // TODO: needs to be atomic with the following SELECT
        }

        // Generate the SELECT statement, based on the options:
        StringBuffer sql = new StringBuffer("SELECT revs.doc_id, docid, revid, sequence");
        if (options.isIncludeDocs()) {
            sql.append(", json, no_attachments");
        }
        if (includeDeletedDocs) {
            sql.append(", deleted");
        }
        sql.append(" FROM revs, docs WHERE");
        if (options.getKeys() != null) {
            if (options.getKeys().size() == 0) {
                return result;
            }
            String commaSeperatedIds = TextUtils.joinQuotedObjects(options.getKeys());
            sql.append(String.format(" revs.doc_id IN (SELECT doc_id FROM docs WHERE docid IN (%s)) AND",
                    commaSeperatedIds));
        }
        sql.append(" docs.doc_id = revs.doc_id AND current=1");
        if (!includeDeletedDocs) {
            sql.append(" AND deleted=0");
        }

        List<String> args = new ArrayList<String>();
        Object minKey = options.getStartKey();
        Object maxKey = options.getEndKey();
        boolean inclusiveMin = true;
        boolean inclusiveMax = options.isInclusiveEnd();
        if (options.isDescending()) {
            minKey = maxKey;
            maxKey = options.getStartKey();
            inclusiveMin = inclusiveMax;
            inclusiveMax = true;
        }
        if (minKey != null) {
            assert (minKey instanceof String);
            sql.append((inclusiveMin ? " AND docid >= ?" : " AND docid > ?"));
            args.add((String) minKey);
        }
        if (maxKey != null) {
            assert (maxKey instanceof String);
            maxKey = View.keyForPrefixMatch(maxKey, options.getPrefixMatchLevel());
            sql.append((inclusiveMax ? " AND docid <= ?" : " AND docid < ?"));
            args.add((String) maxKey);
        }

        sql.append(
                String.format(
                        " ORDER BY docid %s, %s revid DESC LIMIT ? OFFSET ?",
                        (options.isDescending() ? "DESC" : "ASC"),
                        (includeDeletedDocs ? "deleted ASC," : "")
                )
        );

        args.add(Integer.toString(options.getLimit()));
        args.add(Integer.toString(options.getSkip()));

        // Now run the database query:
        Cursor cursor = null;
        Map<String, QueryRow> docs = new HashMap<String, QueryRow>();
        try {
            // Get row values now, before the code below advances 'cursor':
            cursor = storageEngine.rawQuery(sql.toString(), args.toArray(new String[args.size()]));

            boolean keepGoing = cursor.moveToNext(); // Go to first result row
            while (keepGoing) {
                long docNumericID = cursor.getLong(0);
                String docID = cursor.getString(1);
                String revID = cursor.getString(2);
                long sequence = cursor.getLong(3);
                boolean deleted = includeDeletedDocs && cursor.getInt(getDeletedColumnIndex(options)) > 0;
                RevisionInternal docRevision = null;
                if (options.isIncludeDocs()) {
                    //docRevision = revision(docID, revID, deleted, sequence, cursor.getBlob(4));
                    byte[] json = cursor.getBlob(4);
                    Map<String, Object> properties = documentPropertiesFromJSON(json, docID, revID,
                            false, sequence);
                    docRevision = revision(
                            docID,    // docID
                            revID,    // revID
                            false,    // deleted
                            sequence, // sequence
                            properties// properties
                    );
                }

                // Iterate over following rows with the same doc_id -- these are conflicts.
                // Skip them, but collect their revIDs if the 'conflicts' option is set:
                List<String> conflicts = new ArrayList<String>();
                while (((keepGoing = cursor.moveToNext()) == true) && cursor.getLong(0) == docNumericID) {
                    if (options.getAllDocsMode() == Query.AllDocsMode.SHOW_CONFLICTS ||
                            options.getAllDocsMode() == Query.AllDocsMode.ONLY_CONFLICTS) {
                        if (conflicts.isEmpty()) {
                            conflicts.add(revID);
                        }
                        conflicts.add(cursor.getString(2));
                    }
                }

                if (options.getAllDocsMode() == Query.AllDocsMode.ONLY_CONFLICTS && conflicts.isEmpty())
                    continue;

                Map<String, Object> value = new HashMap<String, Object>();
                value.put("rev", revID);
                value.put("_conflicts", conflicts);
                if (includeDeletedDocs) {
                    value.put("deleted", (deleted ? true : null));
                }
                QueryRow change = new QueryRow(docID,
                        sequence,
                        docID,
                        value,
                        docRevision,
                        null);
                if (options.getKeys() != null)
                    docs.put(docID, change);
                    // TODO: In the future, we need to implement CBLRowPassesFilter() in CBLView+Querying.m
                else if (options.getPostFilter() == null || options.getPostFilter().apply(change))
                    rows.add(change);
            }

            // If given doc IDs, sort the output into that order, and add entries for missing docs:
            if (options.getKeys() != null) {
                for (Object docIdObject : options.getKeys()) {
                    if (docIdObject instanceof String) {
                        String docID = (String) docIdObject;
                        QueryRow change = docs.get(docID);
                        if (change == null) {
                            Map<String, Object> value = new HashMap<String, Object>();
                            long docNumericID = getDocNumericID(docID);
                            if (docNumericID > 0) {
                                boolean deleted;
                                AtomicBoolean outIsDeleted = new AtomicBoolean(false);
                                String revID = winningRevIDOfDocNumericID(docNumericID, outIsDeleted, null);
                                if (revID != null) {
                                    value.put("rev", revID);
                                    value.put("deleted", true);
                                }
                            }
                            change = new QueryRow((value != null ? docID : null), 0, docID, value, null, null);
                        }
                        // TODO add options.filter
                        rows.add(change);
                    }
                }
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting all docs", e);
            throw new CouchbaseLiteException("Error getting all docs", e, new Status(Status.INTERNAL_SERVER_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        result.put("rows", rows);
        result.put("total_rows", rows.size());
        result.put("offset", options.getSkip());
        if (updateSeq != 0) {
            result.put("update_seq", updateSeq);
        }

        return result;
    }

    @Override
    public RevisionList changesSince(long lastSequence, ChangesOptions options, ReplicationFilter filter,
                                     Map<String, Object> filterParams) {
        // http://wiki.apache.org/couchdb/HTTP_database_API#Changes
        if (options == null) {
            options = new ChangesOptions();
        }

        boolean includeDocs = options.isIncludeDocs() || (filter != null);
        String additionalSelectColumns = "";
        if (includeDocs) {
            additionalSelectColumns = ", json";
        }

        String sql = "SELECT sequence, revs.doc_id, docid, revid, deleted" + additionalSelectColumns + " FROM revs, docs "
                + "WHERE sequence > ? AND current=1 "
                + "AND revs.doc_id = docs.doc_id "
                + "ORDER BY revs.doc_id, revid DESC";
        String[] args = {Long.toString(lastSequence)};
        Cursor cursor = null;
        RevisionList changes = null;

        try {
            cursor = storageEngine.rawQuery(sql, args);
            cursor.moveToNext();
            changes = new RevisionList();
            long lastDocId = 0;
            while (!cursor.isAfterLast()) {
                if (!options.isIncludeConflicts()) {
                    // Only count the first rev for a given doc (the rest will be losing conflicts):
                    long docNumericId = cursor.getLong(1);
                    if (docNumericId == lastDocId) {
                        cursor.moveToNext();
                        continue;
                    }
                    lastDocId = docNumericId;
                }

                RevisionInternal rev = new RevisionInternal(cursor.getString(2), cursor.getString(3), (cursor.getInt(4) > 0));
                rev.setSequence(cursor.getLong(0));
                if (includeDocs)
                    rev.setJSON(cursor.getBlob(5));
                if (delegate.runFilter(filter, filterParams, rev))
                    changes.add(rev);
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error looking for changes", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        if (options.isSortBySequence()) {
            changes.sortBySequence();
        }
        changes.limit(options.getLimit());
        return changes;
    }

    ///////////////////////////////////////////////////////////////////////////
    // INSERTION / DELETION:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Creates a new revision of a document.
     * On success, before returning the new CBL_Revision, the implementation will also call the
     * delegate's -databaseStorageChanged: method to give it more details about the change.
     *
     * @param docID           The document ID, or nil if an ID should be generated at random.
     * @param prevRevID       The parent revision ID, or nil if creating a new document.
     * @param properties      The new revision's properties. (Metadata other than "_attachments" ignored.)
     * @param deleting        YES if this revision is a deletion.
     * @param allowConflict   YES if this operation is allowed to create a conflict; otherwise a 409,
     *                        status will be returned if the parent revision is not a leaf.
     * @param validationBlock If non-nil, this block will be called before the revision is added.
     *                        It's given the parent revision, with its properties if available, and can reject
     *                        the operation by returning an error status.
     * @param outStatus       On return a status will be stored here. Note that on success, the
     *                        status should be 201 for a created revision but 200 for a deletion.
     * @return The new revision, with its revID and sequence filled in, or nil on error.
     */
    @Override
    @InterfaceAudience.Private
    public RevisionInternal add(
            String docID,
            String prevRevID,
            Map<String, Object> properties,
            boolean deleting,
            boolean allowConflict,
            StorageValidation validationBlock,
            Status outStatus)
            throws CouchbaseLiteException {

        byte[] json;
        if (properties != null && properties.size() > 0) {
            json = RevisionUtils.asCanonicalJSON(properties);
            if (json == null)
                throw new CouchbaseLiteException(Status.BAD_JSON);
        } else {
            json = "{}".getBytes();
        }

        RevisionInternal newRev = null;
        String winningRevID = null;
        boolean inConflict = false;

        beginTransaction();
        // try - finally for beginTransaction() and endTransaction()
        try {
            //// PART I: In which are performed lookups and validations prior to the insert...

            // Get the doc's numeric ID (doc_id) and its current winning revision:
            AtomicBoolean isNewDoc = new AtomicBoolean(prevRevID == null);
            long docNumericID = -1;
            if (docID != null) {
                docNumericID = createOrGetDocNumericID(docID, isNewDoc);
                if (docNumericID <= 0)
                    // TODO: error
                    throw new CouchbaseLiteException(Status.UNKNOWN);
            } else {
                docNumericID = 0;
                isNewDoc.set(true);
            }

            AtomicBoolean oldWinnerWasDeletion = new AtomicBoolean(false);
            AtomicBoolean wasConflicted = new AtomicBoolean(false);
            String oldWinningRevID = null;
            if (!isNewDoc.get()) {
                // Look up which rev is the winner, before this insertion
                //OPT: This rev ID could be cached in the 'docs' row
                oldWinningRevID = winningRevIDOfDocNumericID(docNumericID, oldWinnerWasDeletion, wasConflicted);
            }

            long parentSequence = 0;
            if (prevRevID != null) {
                // Replacing: make sure given prevRevID is current & find its sequence number:
                if (isNewDoc.get())
                    throw new CouchbaseLiteException(Status.NOT_FOUND);

                parentSequence = getSequenceOfDocument(docNumericID, prevRevID, !allowConflict);
                if (parentSequence <= 0) { // -1 if not found
                    // Not found: either a 404 or a 409, depending on whether there is any current revision
                    if (!allowConflict && existsDocument(docID, null)) {
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    } else {
                        throw new CouchbaseLiteException(Status.NOT_FOUND);
                    }
                }
            } else {
                // Inserting first revision.
                if (deleting && docID != null) {
                    // Didn't specify a revision to delete: 404 or a 409, depending
                    if (existsDocument(docID, null))
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    else
                        throw new CouchbaseLiteException(Status.NOT_FOUND);
                }

                if (docID != null) {
                    // Inserting first revision, with docID given (PUT):
                    // Doc ID exists; check whether current winning revision is deleted:
                    if (oldWinnerWasDeletion.get() == true) {
                        prevRevID = oldWinningRevID;
                        parentSequence = getSequenceOfDocument(docNumericID, prevRevID, false);
                    } else if (oldWinningRevID != null) {
                        // The current winning revision is not deleted, so this is a conflict
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    }
                } else {
                    // Inserting first revision, with no docID given (POST): generate a unique docID:
                    docID = Misc.CreateUUID();
                    docNumericID = createOrGetDocNumericID(docID, isNewDoc);
                    if (docNumericID <= 0)
                        return null;
                }
            }

            // There may be a conflict if (a) the document was already in conflict, or
            // (b) a conflict is created by adding a non-deletion child of a non-winning rev.
            inConflict = wasConflicted.get() ||
                    (!deleting &&
                            prevRevID != null &&
                            oldWinningRevID != null &&
                            !prevRevID.equals(oldWinningRevID));

            //// PART II: In which we prepare for insertion...

            // Bump the revID and update the JSON:
            String newRevId = delegate.generateRevID(json, deleting, prevRevID);
            if (newRevId == null)
                throw new CouchbaseLiteException(Status.BAD_ID); // invalid previous revID (no numeric prefix)
            assert (docID != null);
            newRev = new RevisionInternal(docID, newRevId, deleting);
            if (properties != null) {
                properties.put("_id", docID);
                properties.put("_rev", newRevId);
                newRev.setProperties(properties);
            }

            // Validate:
            if (validationBlock != null) {
                // Fetch the previous revision and validate the new one against it:
                RevisionInternal prevRev = null;
                if (prevRevID != null)
                    prevRev = new RevisionInternal(docID, prevRevID, false);
                Status status = validationBlock.validate(newRev, prevRev, prevRevID);
                if (status.isError()) {
                    outStatus.setCode(status.getCode());
                    throw new CouchbaseLiteException(status);
                }
            }

            // Don't store a SQL null in the 'json' column -- I reserve it to mean that the revision data
            // is missing due to compaction or replication.
            // Instead, store an empty zero-length blob.
            if (json == null)
                json = new byte[0];

            //// PART III: In which the actual insertion finally takes place:
            boolean hasAttachments = properties == null ? false : properties.get("_attachments") != null;
            String docType = properties == null ? null : (String) properties.get("type");
            long sequence = insertRevision(newRev,
                    docNumericID,
                    parentSequence,
                    true,
                    hasAttachments,
                    json,
                    docType);
            if (sequence <= 0) {
                // The insert failed. If it was due to a constraint violation, that means a revision
                // already exists with identical contents and the same parent rev. We can ignore this
                // insert call, then.
                // TODO - figure out storageEngine error code
                // NOTE - In AndroidSQLiteStorageEngine.java, CBL Android uses insert() method of SQLiteDatabase
                //        which return -1 for error case. Instead of insert(), should use insertOrThrow method()
                //        which throws detailed error code. Need to check with SQLiteDatabase.CONFLICT_FAIL.
                //        However, returning after updating parentSequence might not cause any problem.
                //if (_fmdb.lastErrorCode != SQLITE_CONSTRAINT)
                //    return null;
                Log.w(TAG, "Duplicate rev insertion: " + docID + " / " + newRevId);
                newRev.setBody(null);
                // don't return yet; update the parent's current just to be sure (see #316 (iOS #509))
            }

            // Make replaced rev non-current:
            if (parentSequence > 0) {
                try {
                    ContentValues args = new ContentValues();
                    args.put("current", 0);
                    args.put("doc_type", (String) null);
                    storageEngine.update("revs", args, "sequence=?", new String[]{String.valueOf(parentSequence)});
                } catch (SQLException e) {
                    Log.e(TAG, "Error setting parent rev non-current", e);
                    storageEngine.delete("revs", "sequence=?", new String[]{String.valueOf(sequence)});
                    throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
                }
            }

            if (sequence <= 0) {
                // duplicate rev; see above
                outStatus.setCode(Status.OK);
                if (newRev.getSequence() != 0)
                    delegate.databaseStorageChanged(new DocumentChange(newRev, winningRevID, inConflict, null));
                return newRev;
            }

            // Figure out what the new winning rev ID is:
            winningRevID = winner(docNumericID, oldWinningRevID, oldWinnerWasDeletion.get(), newRev);

            // Success!
            if (deleting) {
                outStatus.setCode(Status.OK);
            } else {
                outStatus.setCode(Status.CREATED);
            }

        } finally {
            endTransaction(outStatus.isSuccessful());
        }

        //// EPILOGUE: A change notification is sent...
        if (newRev.getSequence() != 0)
            delegate.databaseStorageChanged(new DocumentChange(newRev, winningRevID, inConflict, null));

        return newRev;
    }

    /**
     * Inserts an already-existing revision replicated from a remote sqliteDb.
     * <p/>
     * It must already have a revision ID. This may create a conflict! The revision's history must be given; ancestor revision IDs that don't already exist locally will create phantom revisions with no content.
     *
     * @exclude in CBLDatabase+Insertion.m
     * - (CBLStatus) forceInsert: (CBL_Revision*)inRev
     * revisionHistory: (NSArray*)history  // in *reverse* order, starting with rev's revID
     * source: (NSURL*)source
     */
    @Override
    @InterfaceAudience.Private
    public void forceInsert(RevisionInternal inRev,
                            List<String> history,
                            StorageValidation validationBlock,
                            URL source)
            throws CouchbaseLiteException {

        Status status = new Status(Status.UNKNOWN);

        RevisionInternal rev = inRev.copy();
        rev.setSequence(0);
        String docID = rev.getDocID();

        String winningRevID = null;
        AtomicBoolean inConflict = new AtomicBoolean(false);
        boolean success = false;

        beginTransaction();
        try {
            // First look up the document's row-id and all locally-known revisions of it:
            Map<String, RevisionInternal> localRevs = null;
            String oldWinningRevID = null;
            AtomicBoolean oldWinnerWasDeletion = new AtomicBoolean(false);
            AtomicBoolean isNewDoc = new AtomicBoolean(history.size() == 1);
            long docNumericID = createOrGetDocNumericID(docID, isNewDoc);
            if (docNumericID <= 0)
                throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
            if (!isNewDoc.get()) {
                RevisionList localRevsList = getAllRevisions(docID, docNumericID, false);
                if (localRevsList == null)
                    throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                localRevs = new HashMap<String, RevisionInternal>();
                for (RevisionInternal r : localRevsList)
                    localRevs.put(r.getRevID(), r);

                // Look up which rev is the winner, before this insertion
                oldWinningRevID = winningRevIDOfDocNumericID(
                        docNumericID,
                        oldWinnerWasDeletion,
                        inConflict);
            }

            // Validate against the latest common ancestor:
            if (validationBlock != null) {
                RevisionInternal oldRev = null;
                for (int i = 1; i < history.size(); i++) {
                    oldRev = (localRevs != null) ? localRevs.get(history.get(i)) : null;
                    if (oldRev != null) {
                        break;
                    }
                }
                String parentRevId = (history.size() > 1) ? history.get(1) : null;
                Status tmpStatus = validationBlock.validate(rev, oldRev, parentRevId);
                if (tmpStatus.isError()) {
                    throw new CouchbaseLiteException(tmpStatus);
                }
            }

            // Walk through the remote history in chronological order, matching each revision ID to
            // a local revision. When the list diverges, start creating blank local revisions to
            // fill in the local history:
            long sequence = 0;
            long localParentSequence = 0;
            for (int i = history.size() - 1; i >= 0; --i) {
                String revID = history.get(i);
                RevisionInternal localRev = (localRevs != null) ? localRevs.get(revID) : null;
                if (localRev != null) {
                    // This revision is known locally. Remember its sequence as the parent of
                    // the next one:
                    sequence = localRev.getSequence();
                    assert (sequence > 0);
                    localParentSequence = sequence;
                } else {
                    // This revision isn't known, so add it:
                    RevisionInternal newRev = null;
                    byte[] json = null;
                    String docType = null;
                    boolean current = false;
                    if (i == 0) {
                        // Hey, this is the leaf revision we're inserting:
                        newRev = rev;
                        json = RevisionUtils.asCanonicalJSON(inRev);
                        if (json == null)
                            throw new CouchbaseLiteException(Status.BAD_JSON);
                        docType = (String) rev.getObject("type");
                        current = true;
                    } else {
                        // It's an intermediate parent, so insert a stub:
                        newRev = new RevisionInternal(docID, revID, false);
                    }

                    // Insert it:
                    sequence = insertRevision(
                            newRev,
                            docNumericID,
                            sequence,
                            current,
                            (newRev.getAttachments() != null && newRev.getAttachments().size() > 0),
                            json,
                            docType);
                    if (sequence <= 0)
                        throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                }
            }

            if (localParentSequence == sequence) {
                success = true; // No-op: No new revisions were inserted.
                status.setCode(Status.OK);
            }
            // Mark the latest local rev as no longer current:
            else if (localParentSequence > 0) {
                ContentValues args = new ContentValues();
                args.put("current", 0);
                args.put("doc_type", (String) null);
                String[] whereArgs = {Long.toString(localParentSequence)};
                int numRowsChanged = 0;
                try {
                    numRowsChanged = storageEngine.update("revs", args, "sequence=? AND current!=0", whereArgs);
                    if (numRowsChanged == 0)
                        inConflict.set(true);  // local parent wasn't a leaf, ergo we just created a branch
                } catch (SQLException e) {
                    throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                }
            }

            if (!success) {
                // Figure out what the new winning rev ID is:
                winningRevID = winner(docNumericID, oldWinningRevID, oldWinnerWasDeletion.get(), rev);
                success = true;
                status.setCode(Status.CREATED);
            }
        } catch (SQLException e) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            endTransaction(success);
        }
        // Notify and return:
        if (status.getCode() == Status.CREATED)
            delegate.databaseStorageChanged(new DocumentChange(rev, winningRevID, inConflict.get(), source));
        else if (status.isError())
            throw new CouchbaseLiteException(status);
    }

    /**
     * Purges specific revisions, which deletes them completely from the local storageEngine _without_ adding a "tombstone" revision. It's as though they were never there.
     * This operation is described here: http://wiki.apache.org/couchdb/Purge_Documents
     *
     * @param docsToRevs A dictionary mapping document IDs to arrays of revision IDs.
     * @resultOn success will point to an NSDictionary with the same form as docsToRev, containing the doc/revision IDs that were actually removed.
     */
    @Override
    @InterfaceAudience.Private
    public Map<String, Object> purgeRevisions(final Map<String, List<String>> docsToRevs) {

        final Map<String, Object> result = new HashMap<String, Object>();
        runInTransaction(new TransactionalTask() {
            @Override
            public boolean run() {
                for (String docID : docsToRevs.keySet()) {
                    long docNumericID = getDocNumericID(docID);
                    if (docNumericID == -1) {
                        continue; // no such document, skip it
                    }
                    List<String> revsPurged = new ArrayList<String>();
                    List<String> revIDs = (List<String>) docsToRevs.get(docID);
                    if (revIDs == null) {
                        return false;
                    } else if (revIDs.size() == 0) {
                        revsPurged = new ArrayList<String>();
                    } else if (revIDs.contains("*")) {
                        // Delete all revisions if magic "*" revision ID is given:
                        try {
                            String[] args = {Long.toString(docNumericID)};
                            storageEngine.execSQL("DELETE FROM revs WHERE doc_id=?", args);
                        } catch (SQLException e) {
                            Log.e(TAG, "Error deleting revisions", e);
                            return false;
                        }
                        revsPurged = new ArrayList<String>();
                        revsPurged.add("*");
                    } else {
                        // Iterate over all the revisions of the doc, in reverse sequence order.
                        // Keep track of all the sequences to delete, i.e. the given revs and ancestors,
                        // but not any non-given leaf revs or their ancestors.
                        Cursor cursor = null;

                        try {
                            String[] args = {Long.toString(docNumericID)};
                            String queryString = "SELECT revid, sequence, parent FROM revs WHERE doc_id=? ORDER BY sequence DESC";
                            cursor = storageEngine.rawQuery(queryString, args);
                            if (!cursor.moveToNext()) {
                                Log.w(TAG, "No results for query: %s", queryString);
                                return false;
                            }

                            Set<Long> seqsToPurge = new HashSet<Long>();
                            Set<Long> seqsToKeep = new HashSet<Long>();
                            Set<String> revsToPurge = new HashSet<String>();
                            while (!cursor.isAfterLast()) {

                                String revID = cursor.getString(0);
                                long sequence = cursor.getLong(1);
                                long parent = cursor.getLong(2);
                                if (seqsToPurge.contains(sequence) || revIDs.contains(revID) && !seqsToKeep.contains(sequence)) {
                                    // Purge it and maybe its parent:
                                    seqsToPurge.add(sequence);
                                    revsToPurge.add(revID);
                                    if (parent > 0) {
                                        seqsToPurge.add(parent);
                                    }
                                } else {
                                    // Keep it and its parent:
                                    seqsToPurge.remove(sequence);
                                    revsToPurge.remove(revID);
                                    seqsToKeep.add(parent);
                                }

                                cursor.moveToNext();
                            }

                            seqsToPurge.removeAll(seqsToKeep);
                            Log.i(TAG, "Purging doc '%s' revs (%s); asked for (%s)", docID, revsToPurge, revIDs);
                            if (seqsToPurge.size() > 0) {
                                // Now delete the sequences to be purged.
                                String seqsToPurgeList = TextUtils.join(",", seqsToPurge);
                                String sql = String.format("DELETE FROM revs WHERE sequence in (%s)", seqsToPurgeList);
                                try {
                                    storageEngine.execSQL(sql);
                                } catch (SQLException e) {
                                    Log.e(TAG, "Error deleting revisions via: " + sql, e);
                                    return false;
                                }
                            }
                            revsPurged.addAll(revsToPurge);

                        } catch (SQLException e) {
                            Log.e(TAG, "Error getting revisions", e);
                            return false;
                        } finally {
                            if (cursor != null) {
                                cursor.close();
                            }
                        }
                    }
                    result.put(docID, revsPurged);
                }
                return true;
            }
        });

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////
    // VIEWS:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Instantiates storage for a view.
     *
     * @param name   The name of the view
     * @param create If YES, the view should be created; otherwise it must already exist
     * @return Storage for the view, or nil if create=NO and it doesn't exist.
     */
    public ViewStore getViewStorage(String name, boolean create) {
        return new SQLiteViewStore(this, name, create);
    }

    @Override
    public List<String> getAllViewNames() {
        Cursor cursor = null;
        List<String> result = null;

        try {
            cursor = storageEngine.rawQuery("SELECT name FROM views", null);
            cursor.moveToNext();
            result = new ArrayList<String>();
            while (!cursor.isAfterLast()) {
                //result.add(delegate.getView(cursor.getString(0)));
                result.add(cursor.getString(0));
                cursor.moveToNext();
            }
        } catch (Exception e) {
            Log.e(TAG, "Error getting all views", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////
    // LOCAL DOCS:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public RevisionInternal getLocalDocument(String docID, String revID) {
        // docID already should contain "_local/" prefix
        RevisionInternal result = null;
        Cursor cursor = null;
        try {
            String[] args = {docID};
            cursor = storageEngine.rawQuery("SELECT revid, json FROM localdocs WHERE docid=?", args);
            if (cursor.moveToNext()) {
                String gotRevID = cursor.getString(0);
                if (revID != null && (!revID.equals(gotRevID))) {
                    return null;
                }
                byte[] json = cursor.getBlob(1);
                Map<String, Object> properties = null;
                try {
                    properties = Manager.getObjectMapper().readValue(json, Map.class);
                    properties.put("_id", docID);
                    properties.put("_rev", gotRevID);
                    result = new RevisionInternal(docID, gotRevID, false);
                    result.setProperties(properties);
                } catch (Exception e) {
                    Log.w(TAG, "Error parsing local doc JSON", e);
                    return null;
                }

            }
            return result;
        } catch (SQLException e) {
            Log.e(TAG, "Error getting local document", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public RevisionInternal putLocalRevision(RevisionInternal revision, String prevRevID) throws CouchbaseLiteException {
        String docID = revision.getDocID();
        if (!docID.startsWith("_local/")) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        if (!revision.isDeleted()) {
            // PUT:
            byte[] json = RevisionUtils.asCanonicalJSON(revision);
            String newRevID;
            if (prevRevID != null) {
                int generation = RevisionInternal.generationFromRevID(prevRevID);
                if (generation == 0) {
                    throw new CouchbaseLiteException(Status.BAD_REQUEST);
                }
                newRevID = Integer.toString(++generation) + "-local";
                ContentValues values = new ContentValues();
                values.put("revid", newRevID);
                values.put("json", json);
                String[] whereArgs = {docID, prevRevID};
                try {
                    int rowsUpdated = storageEngine.update("localdocs", values, "docid=? AND revid=?", whereArgs);
                    if (rowsUpdated == 0) {
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    }
                } catch (SQLException e) {
                    throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
                }
            } else {
                newRevID = "1-local";
                ContentValues values = new ContentValues();
                values.put("docid", docID);
                values.put("revid", newRevID);
                values.put("json", json);
                try {
                    storageEngine.insertWithOnConflict("localdocs", null, values, SQLiteStorageEngine.CONFLICT_IGNORE);
                } catch (SQLException e) {
                    throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
                }
            }
            return revision.copyWithDocID(docID, newRevID);
        } else {
            // DELETE:
            deleteLocalDocument(docID, prevRevID);
            return revision;
        }
    }

    @Override
    public RevisionList getAllRevisions(String docID, boolean onlyCurrent) {
        long docNumericId = getDocNumericID(docID);
        if (docNumericId < 0) {
            return null;
        } else if (docNumericId == 0) {
            return new RevisionList();
        } else {
            return getAllRevisions(docID, docNumericId, onlyCurrent);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal (PROTECTED & PRIVATE) METHODS
    ///////////////////////////////////////////////////////////////////////////

    protected SQLiteStorageEngine getStorageEngine() {
        return storageEngine;
    }

    private boolean existsDocument(String docID, String revID) {
        return getDocument(docID, revID, false) != null;
    }

    private void deleteLocalDocument(String docID, String revID) throws CouchbaseLiteException {
        if (docID == null) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }
        if (revID == null) {
            // Didn't specify a revision to delete: 404 or a 409, depending
            if (getLocalDocument(docID, null) != null) {
                throw new CouchbaseLiteException(Status.CONFLICT);
            } else {
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            }
        }
        String[] whereArgs = {docID, revID};
        try {
            int rowsDeleted = storageEngine.delete("localdocs", "docid=? AND revid=?", whereArgs);
            if (rowsDeleted == 0) {
                if (getLocalDocument(docID, null) != null) {
                    throw new CouchbaseLiteException(Status.CONFLICT);
                } else {
                    throw new CouchbaseLiteException(Status.NOT_FOUND);
                }
            }
        } catch (SQLException e) {
            throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Returns the rev ID of the 'winning' revision of this document, and whether it's deleted.
     * <p/>
     * in CBLDatabase+Internal.m
     * - (NSString*) winningRevIDOfDocNumericID: (SInt64)docNumericID
     * isDeleted: (BOOL*)outIsDeleted
     * isConflict: (BOOL*)outIsConflict // optional
     * status: (CBLStatus*)outStatus
     */
    protected String winningRevIDOfDocNumericID(long docNumericId,
                                                AtomicBoolean outIsDeleted,
                                                AtomicBoolean outIsConflict) // optional
            throws CouchbaseLiteException {
        assert (docNumericId > 0);
        Cursor cursor = null;
        String sql = "SELECT revid, deleted FROM revs" +
                " WHERE doc_id=? and current=1" +
                " ORDER BY deleted asc, revid desc LIMIT ?";

        long limit = (outIsConflict != null && outIsConflict.get()) ? 2 : 1;
        String[] args = {Long.toString(docNumericId), Long.toString(limit)};
        String revID = null;
        try {
            cursor = storageEngine.rawQuery(sql, args);
            if (cursor.moveToNext()) {
                revID = cursor.getString(0);
                outIsDeleted.set(cursor.getInt(1) > 0);
                // The document is in conflict if there are two+ result rows that are not deletions.
                if (outIsConflict != null) {
                    outIsConflict.set(!outIsDeleted.get() && cursor.moveToNext() && !(cursor.getInt(1) > 0));
                }
            } else {
                outIsDeleted.set(false);
                if (outIsConflict != null) {
                    outIsConflict.set(false);
                }
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error", e);
            throw new CouchbaseLiteException("Error", e, new Status(Status.INTERNAL_SERVER_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return revID;
    }

    /**
     * https://github.com/couchbase/couchbase-lite-ios/issues/615
     */
    protected void optimizeSQLIndexes() {
        Log.v(Log.TAG_DATABASE, "calls optimizeSQLIndexes()");
        final long currentSequence = getLastSequence();
        if (currentSequence > 0) {
            final long lastOptimized = getLastOptimized();
            if (lastOptimized <= currentSequence / 10) {
                runInTransaction(new TransactionalTask() {
                    @Override
                    public boolean run() {
                        Log.i(Log.TAG_DATABASE, "%s: Optimizing SQL indexes (curSeq=%d, last run at %d)",
                                this, currentSequence, lastOptimized);
                        storageEngine.execSQL("ANALYZE");
                        storageEngine.execSQL("ANALYZE sqlite_master");
                        setInfo("last_optimized", String.valueOf(currentSequence));
                        return true;
                    }
                });
            }
        }
    }

    /**
     * Begins a storageEngine transaction. Transactions can nest.
     * Every beginTransaction() must be balanced by a later endTransaction()
     * <p/>
     * Notes: 1. SQLiteDatabase.beginTransaction() supported nested transaction. But, in case
     * nested transaction rollbacked, it also rollbacks outer transaction too.
     * This is not ideal behavior for CBL
     * 2. SAVEPOINT...RELEASE supports nested transaction. But Android API 14 and 15,
     * it throws Exception. I assume it is Android bug. As CBL need to support from API 10
     * . So it does not work for CBL.
     * 3. Using Transaction for outer/1st level of transaction and inner/2nd level of transaction
     * works with CBL requirement.
     * 4. CBL Android and Java uses Thread, So it is better to use SQLiteDatabase.beginTransaction()
     * for outer/1st level transaction. if we use BEGIN...COMMIT and SAVEPOINT...RELEASE,
     * we need to implement wrapper of BEGIN...COMMIT and SAVEPOINT...RELEASE to be
     * Thread-safe.
     */
    protected boolean beginTransaction() {
        int tLevel = transactionLevel.get();
        try {
            // Outer (level 0)  transaction. Use SQLiteDatabase.beginTransaction()
            if (tLevel == 0) {
                storageEngine.beginTransaction();
            }
            // Inner (level 1 or higher) transaction. Use SQLite's SAVEPOINT
            else {
                storageEngine.execSQL("SAVEPOINT cbl_" + Integer.toString(tLevel));
            }
            Log.v(Log.TAG_DATABASE, "%s Begin transaction (level %d)", Thread.currentThread().getName(), tLevel);
            transactionLevel.set(++tLevel);
        } catch (SQLException e) {
            Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling beginTransaction()", e);
            return false;
        }
        return true;
    }

    /**
     * Commits or aborts (rolls back) a transaction.
     *
     * @param commit If true, commits; if false, aborts and rolls back, undoing all changes made
     *               since the matching -beginTransaction call, *including* any committed nested
     *               transactions.
     * @exclude
     */
    protected boolean endTransaction(boolean commit) {
        int tLevel = transactionLevel.get();

        assert (tLevel > 0);

        transactionLevel.set(--tLevel);

        // Outer (level 0) transaction. Use SQLiteDatabase.setTransactionSuccessful() and SQLiteDatabase.endTransaction()
        if (tLevel == 0) {
            if (commit) {
                Log.v(Log.TAG_DATABASE, "%s Committing transaction (level %d)", Thread.currentThread().getName(), tLevel);
                storageEngine.setTransactionSuccessful();
                storageEngine.endTransaction();
            } else {
                Log.v(Log.TAG_DATABASE, "%s CANCEL transaction (level %d)", Thread.currentThread().getName(), tLevel);
                try {
                    storageEngine.endTransaction();
                } catch (SQLException e) {
                    Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling endTransaction()", e);
                    return false;
                }
            }
        }
        // Inner (level 1 or higher) transaction: Use SQLite's ROLLBACK and RELEASE
        else {
            if (commit) {
                Log.v(Log.TAG_DATABASE, "%s Committing transaction (level %d)", Thread.currentThread().getName(), tLevel);
            } else {
                Log.v(Log.TAG_DATABASE, "%s CANCEL transaction (level %d)", Thread.currentThread().getName(), tLevel);
                try {
                    storageEngine.execSQL(";ROLLBACK TO cbl_" + Integer.toString(tLevel));
                } catch (SQLException e) {
                    Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling endTransaction()", e);
                    return false;
                }
            }
            try {
                storageEngine.execSQL("RELEASE cbl_" + Integer.toString(tLevel));
            } catch (SQLException e) {
                Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling endTransaction()", e);
                return false;
            }
        }

        if (delegate != null)
            delegate.storageExitedTransaction(commit);

        return true;
    }

    protected Map<String, Object> documentPropertiesFromJSON(byte[] json, String docID,
                                                             String revID, boolean deleted,
                                                             long sequence) {

        RevisionInternal rev = new RevisionInternal(docID, revID, deleted);
        rev.setSequence(sequence);
        rev.setMissing(json == null);
        Map<String, Object> docProperties = null;
        if (json == null || json.length == 0 || (json.length == 2 && json.equals("{}"))) {
            docProperties = new HashMap<String, Object>();
        } else {
            try {
                docProperties = Manager.getObjectMapper().readValue(json, Map.class);
            } catch (IOException e) {
                Log.e(TAG, String.format("Unparseable JSON for doc=%s, rev=%s: %s", docID, revID, new String(json)), e);
                docProperties = new HashMap<String, Object>();
            }
        }

        docProperties.put("_id", docID);
        docProperties.put("_rev", revID);
        if (deleted)
            docProperties.put("_deleted", true);

        return docProperties;
    }

    /**
     * Prune revisions to the given max depth.  Eg, remove revisions older than that max depth,
     * which will reduce storage requirements.
     * <p/>
     * TODO: This implementation is a bit simplistic. It won't do quite the right thing in
     * histories with branches, if one branch stops much earlier than another. The shorter branch
     * will be deleted entirely except for its leaf revision. A more accurate pruning
     * would require an expensive full tree traversal. Hopefully this way is good enough.
     */
    protected int pruneRevsToMaxDepth(int maxDepth) throws CouchbaseLiteException {

        int outPruned = 0;
        boolean shouldCommit = false;
        Map<Long, Integer> toPrune = new HashMap<Long, Integer>();

        if (maxDepth == 0) {
            maxDepth = getMaxRevTreeDepth();
        }

        // First find which docs need pruning, and by how much:

        Cursor cursor = null;
        String[] args = {};

        long docNumericID = -1;
        int minGen = 0;
        int maxGen = 0;

        try {

            cursor = storageEngine.rawQuery(
                    "SELECT doc_id, MIN(revid), MAX(revid) FROM revs GROUP BY doc_id", args);

            while (cursor.moveToNext()) {
                docNumericID = cursor.getLong(0);
                String minGenRevId = cursor.getString(1);
                String maxGenRevId = cursor.getString(2);
                minGen = Revision.generationFromRevID(minGenRevId);
                maxGen = Revision.generationFromRevID(maxGenRevId);
                if ((maxGen - minGen + 1) > maxDepth) {
                    toPrune.put(docNumericID, (maxGen - minGen));
                }
            }

            beginTransaction();

            if (toPrune.size() == 0) {
                return 0;
            }

            for (Long docNumericIDLong : toPrune.keySet()) {
                String minIDToKeep = String.format("%d-", toPrune.get(docNumericIDLong).intValue() + 1);
                String[] deleteArgs = {Long.toString(docNumericID), minIDToKeep};
                int rowsDeleted = storageEngine.delete(
                        "revs", "doc_id=? AND revid < ? AND current=0", deleteArgs);
                outPruned += rowsDeleted;
            }

            shouldCommit = true;

        } catch (Exception e) {
            throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
        } finally {
            endTransaction(shouldCommit);
            if (cursor != null) {
                cursor.close();
            }
        }

        return outPruned;
    }

    protected boolean runStatements(String statements) {
        for (String statement : statements.split(";")) {
            try {
                storageEngine.execSQL(statement);
            } catch (SQLException e) {
                Log.e(TAG, "Failed to execSQL: " + statement, e);
                return false;
            }
        }
        return true;
    }

    private boolean initialize(String statements) {
        if (!runStatements(statements)) {
            close();
            return false;
        }
        return true;
    }

    private long getLastOptimized() {
        String info = getInfo("last_optimized");
        if (info != null) {
            return Long.parseLong(info);
        }
        return 0;
    }

    private boolean sequenceHasAttachments(long sequence) {
        String args[] = {Long.toString(sequence)};
        return SQLiteUtils.booleanForQuery(storageEngine, "SELECT no_attachments=0 FROM revs WHERE sequence=?", args);
    }

    protected long getDocNumericID(String docID) {
        return SQLiteUtils.longForQuery(storageEngine,
                "SELECT doc_id FROM docs WHERE docid=?",
                new String[]{docID});
    }

    // Registers a docID and returns its numeric row ID in the 'docs' table.
    // On input, *ioIsNew should be YES if the docID is probably not known, NO if it's probably known.
    // On return, *ioIsNew will be YES iff the docID is newly-created (was not known before.)
    // Return value is the positive row ID of this doc, or <= 0 on error.
    private long createOrGetDocNumericID(String docID, AtomicBoolean isNew) {
        // TODO: cache

        long row = isNew.get() ? createDocNumericID(docID, isNew) : getDocNumericID(docID);
        if (row < 0)
            return row;
        if (row == 0) {
            isNew.set(!isNew.get());
            row = isNew.get() ? createDocNumericID(docID, isNew) : getDocNumericID(docID);
        }

        // TODO: cache

        return row;
    }

    //private long getOrInsertDocNumericID(String docID) {
    private long createDocNumericID(String docID, AtomicBoolean isNew) {
        long docNumericId = getDocNumericID(docID);
        if (docNumericId == 0) {
            docNumericId = insertDocumentID(docID);
            isNew.set(true);
        } else {
            isNew.set(false);
        }
        return docNumericId;
    }

    private long insertDocumentID(String docID) {
        long rowId = -1;
        try {
            ContentValues args = new ContentValues();
            args.put("docid", docID);
            rowId = storageEngine.insert("docs", null, args);
        } catch (Exception e) {
            Log.e(TAG, "Error inserting document id", e);
        }
        return rowId;
    }

    // Raw row insertion. Returns new sequence, or 0 on error
    private long insertRevision(RevisionInternal rev,
                                long docNumericID,
                                long parentSequence,
                                boolean current,
                                boolean hasAttachments,
                                byte[] json,
                                String docType) {
        long rowId = 0;
        try {
            ContentValues args = new ContentValues();
            args.put("doc_id", docNumericID);
            args.put("revid", rev.getRevID());
            if (parentSequence != 0) {
                args.put("parent", parentSequence);
            }
            args.put("current", current);
            args.put("deleted", rev.isDeleted());
            args.put("no_attachments", !hasAttachments);
            args.put("json", json);
            args.put("doc_type", docType);
            rowId = storageEngine.insert("revs", null, args);
            rev.setSequence(rowId);
        } catch (Exception e) {
            Log.e(TAG, "Error inserting revision", e);
        }
        return rowId;
    }

    private long getSequenceOfDocument(long docNumericID, String revID, boolean onlyCurrent) {
        String sql = String.format(
                "SELECT sequence FROM revs WHERE doc_id=? AND revid=? %s LIMIT 1",
                (onlyCurrent ? "AND current=1" : ""));
        String[] args = {Long.toString(docNumericID), revID};
        return SQLiteUtils.longForQuery(storageEngine, sql, args);
    }

    /**
     * Hack because cursor interface does not support cursor.getColumnIndex("deleted") yet.
     */
    private int getDeletedColumnIndex(QueryOptions options) {
        if (options.isIncludeDocs()) {
            return 6; // + json and no_attachments
        } else {
            return 4; // revs.doc_id, docid, revid, sequence
        }
    }

    /**
     * Loads revision given its sequence. Assumes the given docID is valid.
     */
    protected RevisionInternal getDocument(String docID, long sequence) {
        // Now get its revID and deletion status:
        RevisionInternal rev = null;

        String[] args = {Long.toString(sequence)};
        String queryString = "SELECT revid, deleted, json FROM revs WHERE sequence=?";
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(queryString, args);
            if (cursor.moveToNext()) {
                String revID = cursor.getString(0);
                boolean deleted = (cursor.getInt(1) > 0);
                byte[] json = cursor.getBlob(2);
                rev = new RevisionInternal(docID, revID, deleted);
                rev.setSequence(sequence);
                rev.setJSON(json);
            }
        } finally {
            cursor.close();
        }
        return rev;
    }

    protected RevisionInternal revision(String docID, String revID,
                                        boolean deleted, long sequence, byte[] json) {
        RevisionInternal rev = new RevisionInternal(docID, revID, deleted);
        rev.setSequence(sequence);
        if (json != null)
            rev.setJSON(json);
        return rev;
    }

    protected RevisionInternal revision(String docID, String revID,
                                        boolean deleted, long sequence,
                                        Map<String, Object> properties) {
        RevisionInternal rev = new RevisionInternal(docID, revID, deleted);
        rev.setSequence(sequence);
        if (properties != null)
            rev.setProperties(properties);
        return rev;
    }

    private String winner(long docNumericID,
                          String oldWinningRevID,
                          boolean oldWinnerWasDeletion,
                          RevisionInternal newRev)
            throws CouchbaseLiteException {

        String newRevID = newRev.getRevID();
        if (oldWinningRevID == null) {
            return newRevID;
        }
        if (!newRev.isDeleted()) {
            if (oldWinnerWasDeletion || RevisionInternal.CBLCompareRevIDs(newRevID, oldWinningRevID) > 0)
                return newRevID; // this is now the winning live revision
        } else if (oldWinnerWasDeletion) {
            if (RevisionInternal.CBLCompareRevIDs(newRevID, oldWinningRevID) > 0)
                return newRevID; // doc still deleted, but this beats previous deletion rev
        } else {
            // Doc was alive. How does this deletion affect the winning rev ID?
            AtomicBoolean outIsDeleted = new AtomicBoolean(false);
            String winningRevID = winningRevIDOfDocNumericID(docNumericID, outIsDeleted, null);
            if (!winningRevID.equals(oldWinningRevID))
                return winningRevID;
        }
        return null; // no change
    }
}