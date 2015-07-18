/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 *
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
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

package com.couchbase.lite;

import com.couchbase.lite.internal.AttachmentInternal;
import com.couchbase.lite.internal.Body;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.replicator.Replication;
import com.couchbase.lite.replicator.ReplicationState;
import com.couchbase.lite.storage.ContentValues;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;
import com.couchbase.lite.storage.SQLiteStorageEngineFactory;
import com.couchbase.lite.support.Base64;
import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.PersistentCookieStore;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.StreamUtils;
import com.couchbase.lite.util.TextUtils;
import com.couchbase.lite.util.Utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A CouchbaseLite database.
 */
public class Database {

    // Default value for maxRevTreeDepth, the max rev depth to preserve in a prune operation
    private static final int DEFAULT_MAX_REVS = Integer.MAX_VALUE;

    // When this many changes pile up in _changesToNotify, start removing their bodies to save RAM
    private static final int MANY_CHANGES_TO_NOTIFY = 5000;

    private static ReplicationFilterCompiler filterCompiler;

    private String path;
    private String name;
    private SQLiteStorageEngine database;

    private boolean open = false;

    // transactionLevel is per thread
    static class TransactionLevel extends ThreadLocal<Integer> {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    }

    private TransactionLevel transactionLevel = new TransactionLevel();


    /**
     * @exclude
     */
    public static final String TAG = Log.TAG;

    private Map<String, View> views;
    private Map<String, ReplicationFilter> filters;
    private Map<String, Validator> validations;

    private Map<String, BlobStoreWriter> pendingAttachmentsByDigest;
    private Set<Replication> activeReplicators;
    private Set<Replication> allReplicators;

    private BlobStore attachments;
    private Manager manager;
    final private CopyOnWriteArrayList<ChangeListener> changeListeners;
    final private CopyOnWriteArrayList<DatabaseListener> databaseListeners;
    private Cache<String, Document> docCache;
    private List<DocumentChange> changesToNotify;
    private boolean postingChangeNotifications;

    /**
     * Each database can have an associated PersistentCookieStore,
     * where the persistent cookie store uses the database to store
     * its cookies.
     *
     * There are two reasons this has been made an instance variable
     * of the Database, rather than of the Replication:
     *
     * - The PersistentCookieStore needs to span multiple replications.
     * For example, if there is a "push" and a "pull" replication for
     * the same DB, they should share a cookie store.
     *
     * - PersistentCookieStore lifecycle should be tied to the Database
     * lifecycle, since it needs to cease to exist if the underlying
     * Database ceases to exist.
     *
     * REF: https://github.com/couchbase/couchbase-lite-android/issues/269
     */
    private PersistentCookieStore persistentCookieStore;

    private int maxRevTreeDepth = DEFAULT_MAX_REVS;

    private long startTime;

    /**
     * Length that constitutes a 'big' attachment
     * @exclude
     */
    public static int kBigAttachmentLength = (16*1024);

    /**
     * Options for what metadata to include in document bodies
     * @exclude
     */
    public enum TDContentOptions {
        TDIncludeAttachments, TDIncludeConflicts, TDIncludeRevs, TDIncludeRevsInfo, TDIncludeLocalSeq, TDNoBody, TDBigAttachmentsFollow, TDNoAttachments
    }

    private static final Set<String> KNOWN_SPECIAL_KEYS;

    static {
        KNOWN_SPECIAL_KEYS = new HashSet<String>();
        KNOWN_SPECIAL_KEYS.add("_id");
        KNOWN_SPECIAL_KEYS.add("_rev");
        KNOWN_SPECIAL_KEYS.add("_attachments");
        KNOWN_SPECIAL_KEYS.add("_deleted");
        KNOWN_SPECIAL_KEYS.add("_revisions");
        KNOWN_SPECIAL_KEYS.add("_revs_info");
        KNOWN_SPECIAL_KEYS.add("_conflicts");
        KNOWN_SPECIAL_KEYS.add("_deleted_conflicts");
        KNOWN_SPECIAL_KEYS.add("_local_seq");
        KNOWN_SPECIAL_KEYS.add("_removed");
    }

    /**
     * @exclude
     */
    // First-time initialization:
    // (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
    // monotonically increasing, never reused. See <http://www.sqlite.org/autoinc.html>)
    public static final String SCHEMA = "" +
            "CREATE TABLE docs ( " +
            "        doc_id INTEGER PRIMARY KEY, " +
            "        docid TEXT UNIQUE NOT NULL); " +
            "    CREATE INDEX docs_docid ON docs(docid); " +
            "    CREATE TABLE revs ( " +
            "        sequence INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "        doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE, " +
            "        revid TEXT NOT NULL COLLATE REVID, " +
            "        parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL, " +
            "        current BOOLEAN, " +
            "        deleted BOOLEAN DEFAULT 0, " +
            "        json BLOB, " +
            "        UNIQUE (doc_id, revid)); " +
            "    CREATE INDEX revs_current ON revs(doc_id, current); " +
            "    CREATE INDEX revs_parent ON revs(parent); " +
            "    CREATE TABLE localdocs ( " +
            "        docid TEXT UNIQUE NOT NULL, " +
            "        revid TEXT NOT NULL COLLATE REVID, " +
            "        json BLOB); " +
            "    CREATE INDEX localdocs_by_docid ON localdocs(docid); " +
            "    CREATE TABLE views ( " +
            "        view_id INTEGER PRIMARY KEY, " +
            "        name TEXT UNIQUE NOT NULL," +
            "        version TEXT, " +
            "        lastsequence INTEGER DEFAULT 0); " +
            "    CREATE INDEX views_by_name ON views(name); " +
            "    CREATE TABLE maps ( " +
            "        view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, " +
            "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
            "        key TEXT NOT NULL COLLATE JSON, " +
            "        value TEXT); " +
            "    CREATE INDEX maps_keys on maps(view_id, key COLLATE JSON); " +
            "    CREATE TABLE attachments ( " +
            "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
            "        filename TEXT NOT NULL, " +
            "        key BLOB NOT NULL, " +
            "        type TEXT, " +
            "        length INTEGER NOT NULL, " +
            "        revpos INTEGER DEFAULT 0); " +
            "    CREATE INDEX attachments_by_sequence on attachments(sequence, filename); " +
            "    CREATE TABLE replicators ( " +
            "        remote TEXT NOT NULL, " +
            "        push BOOLEAN, " +
            "        last_sequence TEXT, " +
            "        UNIQUE (remote, push)); " +
            "    PRAGMA user_version = 3";             // at the end, update user_version


    /**
     * Returns the currently registered filter compiler (nil by default).
     */
    @InterfaceAudience.Public
    public static ReplicationFilterCompiler getFilterCompiler() {
        return filterCompiler;
    }

    /**
     * Registers an object that can compile source code into executable filter blocks.
     */
    @InterfaceAudience.Public
    public static void setFilterCompiler(ReplicationFilterCompiler filterCompiler) {
        Database.filterCompiler = filterCompiler;
    }

    /**
     * Constructor
     * @exclude
     */
    @InterfaceAudience.Private
    public Database(String path, Manager manager) {
        assert(new File(path).isAbsolute()); //path must be absolute
        this.path = path;
        this.name = FileDirUtils.getDatabaseNameFromPath(path);
        this.manager = manager;
        this.changeListeners = new CopyOnWriteArrayList<ChangeListener>();
        this.databaseListeners = new CopyOnWriteArrayList<DatabaseListener>();
        this.docCache = new Cache<String, Document>();
        this.startTime = System.currentTimeMillis();
        this.changesToNotify = new ArrayList<DocumentChange>();
        this.activeReplicators =  Collections.synchronizedSet(new HashSet());
        this.allReplicators = Collections.synchronizedSet(new HashSet());
    }

    /**
     * Get the database's name.
     */
    @InterfaceAudience.Public
    public String getName() {
        return name;
    }

    /**
     * The database manager that owns this database.
     */
    @InterfaceAudience.Public
    public Manager getManager() {
        return manager;
    }

    /**
     * The number of documents in the database.
     */
    @InterfaceAudience.Public
    public int getDocumentCount() {
        String sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
        Cursor cursor = null;
        int result = 0;
        try {
            cursor = database.rawQuery(sql, null);
            if(cursor.moveToNext()) {
                result = cursor.getInt(0);
            }
        } catch(SQLException e) {
            Log.e(Database.TAG, "Error getting document count", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /**
     * The latest sequence number used.  Every new revision is assigned a new sequence number,
     * so this property increases monotonically as changes are made to the database. It can be
     * used to check whether the database has changed between two points in time.
     */
    @InterfaceAudience.Public
    public long getLastSequenceNumber() {
        String sql = "SELECT MAX(sequence) FROM revs";
        Cursor cursor = null;
        long result = 0;
        try {
            cursor = database.rawQuery(sql, null);
            if(cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting last sequence", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * Get all the replicators associated with this database.
     */
    @InterfaceAudience.Public
    public List<Replication> getAllReplications() {
        List<Replication> allReplicatorsList =  new ArrayList<Replication>();
        if (allReplicators != null) {
            allReplicatorsList.addAll(allReplicators);
        }
        return allReplicatorsList;
    }

    /**
     * Compacts the database file by purging non-current JSON bodies, pruning revisions older than
     * the maxRevTreeDepth, deleting unused attachment files, and vacuuming the SQLite database.
     */
    @InterfaceAudience.Public
    public void compact() throws CouchbaseLiteException {
        // Can't delete any rows because that would lose revision tree history.
        // But we can remove the JSON of non-current revisions, which is most of the space.
        try {
            Log.v(Database.TAG, "Pruning old revisions...");
            pruneRevsToMaxDepth(0);
            Log.v(Database.TAG, "Deleting JSON of old revisions...");
            ContentValues args = new ContentValues();
            args.put("json", (String)null);
            database.update("revs", args, "current=0", null);
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error compacting", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }

        Log.v(Database.TAG, "Deleting old attachments...");
        Status result = garbageCollectAttachments();
        if (!result.isSuccessful()) {
            throw new CouchbaseLiteException(result);
        }

        Log.v(Database.TAG, "Vacuuming SQLite sqliteDb...");
        try {
            database.execSQL("VACUUM");
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error vacuuming sqliteDb", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }


    }


    /**
     * Deletes the database.
     *
     * @throws java.lang.RuntimeException
     */
    @InterfaceAudience.Public
    public void delete() throws CouchbaseLiteException {
        if(open) {
            if(!close()) {
                throw new CouchbaseLiteException("The database was open, and could not be closed", Status.INTERNAL_SERVER_ERROR);
            }
        }
        manager.forgetDatabase(this);
        if(!exists()) {
            return;
        }
        File file = new File(path);
        File fileJournal = new File(path + "-journal");

        boolean deleteStatus = file.delete();
        if (fileJournal.exists()) {
            deleteStatus &= fileJournal.delete();
        }

        File attachmentsFile = new File(getAttachmentStorePath());


        //recursively delete attachments path
        boolean deleteAttachmentStatus = FileDirUtils.deleteRecursive(attachmentsFile);

        if (!deleteStatus) {
            throw new CouchbaseLiteException("Was not able to delete the database file", Status.INTERNAL_SERVER_ERROR);
        }

        if (!deleteAttachmentStatus) {
            throw new CouchbaseLiteException("Was not able to delete the attachments files", Status.INTERNAL_SERVER_ERROR);
        }

    }


    /**
     * Instantiates a Document object with the given ID.
     * Doesn't touch the on-disk sqliteDb; a document with that ID doesn't
     * even need to exist yet. CBLDocuments are cached, so there will
     * never be more than one instance (in this sqliteDb) at a time with
     * the same documentID.
     *
     * NOTE: the caching described above is not implemented yet
     *
     * @param documentId
     * @return
     */
    @InterfaceAudience.Public
    public Document getDocument(String documentId) {

        if (documentId == null || documentId.length() == 0) {
            return null;
        }
        Document doc = docCache.get(documentId);
        if (doc == null) {
            doc = new Document(this, documentId);
            if (doc == null) {
                return null;
            }
            docCache.put(documentId, doc);
        }
        return doc;
    }

    /**
     * Gets the Document with the given id, or null if it does not exist.
     */
    @InterfaceAudience.Public
    public Document getExistingDocument(String documentId) {
        if (documentId == null || documentId.length() == 0) {
            return null;
        }
        RevisionInternal revisionInternal = getDocumentWithIDAndRev(documentId, null, EnumSet.noneOf(Database.TDContentOptions.class));
        if (revisionInternal == null) {
            return null;
        }
        return getDocument(documentId);
    }

    /**
     * Creates a new Document object with no properties and a new (random) UUID.
     * The document will be saved to the database when you call -createRevision: on it.
     */
    @InterfaceAudience.Public
    public Document createDocument() {
        return getDocument(Misc.TDCreateUUID());
    }

    private Document cachedDocumentWithID(String documentId){
        return docCache.resourceWithCacheKeyDontRecache(documentId);
    }

    /**
     * Returns the contents of the local document with the given ID, or nil if none exists.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getExistingLocalDocument(String documentId) {
        RevisionInternal revInt = getLocalDocument(makeLocalDocumentId(documentId), null);
        if (revInt == null) {
            return null;
        }
        return revInt.getProperties();
    }

    /**
     * Sets the contents of the local document with the given ID. Unlike CouchDB, no revision-ID
     * checking is done; the put always succeeds. If the properties dictionary is nil, the document
     * will be deleted.
     */
    @InterfaceAudience.Public
    public boolean putLocalDocument(String id, Map<String, Object> properties) throws CouchbaseLiteException {
        // TODO: the iOS implementation wraps this in a transaction, this should do the same.
        id = makeLocalDocumentId(id);
        RevisionInternal prevRev = getLocalDocument(id, null);
        if (prevRev == null && properties == null) {
            return false;
        }
        boolean deleted = false;
        if (properties == null) {
            deleted = true;
        }
        RevisionInternal rev = new RevisionInternal(id, null, deleted);

        if (properties != null) {
            rev.setProperties(properties);
        }

        if (prevRev == null) {
            return putLocalRevision(rev, null) != null;
        } else {
            return putLocalRevision(rev, prevRev.getRevId()) != null;
        }
    }

    /**
     * Deletes the local document with the given ID.
     */
    @InterfaceAudience.Public
    public boolean deleteLocalDocument(String id) throws CouchbaseLiteException {
        id = makeLocalDocumentId(id);
        RevisionInternal prevRev = getLocalDocument(id, null);
        if (prevRev == null) {
            return false;
        }
        deleteLocalDocument(id, prevRev.getRevId());
        return true;
    }

    /**
     * Returns a query that matches all documents in the database.
     * This is like querying an imaginary view that emits every document's ID as a key.
     */
    @InterfaceAudience.Public
    public Query createAllDocumentsQuery() {
        return new Query(this, (View)null);
    }

    /**
     * Returns a View object for the view with the given name.
     * (This succeeds even if the view doesn't already exist, but the view won't be added to
     * the database until the View is assigned a map function.)
     */
    @InterfaceAudience.Public
    public View getView(String name) {
        View view = null;
        if(views != null) {
            view = views.get(name);
        }
        if(view != null) {
            return view;
        }
        return registerView(new View(this, name));
    }

    /**
     * Returns the existing View with the given name, or nil if none.
     */
    @InterfaceAudience.Public
    public View getExistingView(String name) {
        View view = null;
        if(views != null) {
            view = views.get(name);
        }
        if(view != null) {
            return view;
        }

        //view is not in cache but it maybe in DB
        view = new View(this, name);
        if(view.getViewId() > 0) {
            return view;
        }

        return null;
    }

    /**
     * Returns the existing document validation function (block) registered with the given name.
     * Note that validations are not persistent -- you have to re-register them on every launch.
     */
    @InterfaceAudience.Public
    public Validator getValidation(String name) {
        Validator result = null;
        if(validations != null) {
            result = validations.get(name);
        }
        return result;
    }

    /**
     * Defines or clears a named document validation function.
     * Before any change to the database, all registered validation functions are called and given a
     * chance to reject it. (This includes incoming changes from a pull replication.)
     */
    @InterfaceAudience.Public
    public void setValidation(String name, Validator validator) {
        if(validations == null) {
            validations = new HashMap<String, Validator>();
        }
        if (validator != null) {
            validations.put(name, validator);
        }
        else {
            validations.remove(name);
        }
    }

    /**
     * Returns the existing filter function (block) registered with the given name.
     * Note that filters are not persistent -- you have to re-register them on every launch.
     */
    @InterfaceAudience.Public
    public ReplicationFilter getFilter(String filterName) {
        ReplicationFilter result = null;
        if(filters != null) {
            result = filters.get(filterName);
        }
        if (result == null) {
            ReplicationFilterCompiler filterCompiler = getFilterCompiler();
            if (filterCompiler == null) {
                return null;
            }

            List<String> outLanguageList = new ArrayList<String>();
            String sourceCode = getDesignDocFunction(filterName, "filters", outLanguageList);
            if (sourceCode == null) {
                return null;
            }
            String language = outLanguageList.get(0);
            ReplicationFilter filter = filterCompiler.compileFilterFunction(sourceCode, language);
            if (filter == null) {
                Log.w(Database.TAG, "Filter %s failed to compile", filterName);
                return null;
            }
            setFilter(filterName, filter);
            return filter;
        }
        return result;
    }

    /**
     * Define or clear a named filter function.
     *
     * Filters are used by push replications to choose which documents to send.
     */
    @InterfaceAudience.Public
    public void setFilter(String filterName, ReplicationFilter filter) {
        if(filters == null) {
            filters = new HashMap<String,ReplicationFilter>();
        }
        if (filter != null) {
            filters.put(filterName, filter);
        }
        else {
            filters.remove(filterName);
        }
    }

    /**
     * Runs the block within a transaction. If the block returns NO, the transaction is rolled back.
     * Use this when performing bulk write operations like multiple inserts/updates;
     * it saves the overhead of multiple SQLite commits, greatly improving performance.
     *
     * Does not commit the transaction if the code throws an Exception.
     *
     * TODO: the iOS version has a retry loop, so there should be one here too
     *
     * @param transactionalTask
     */
    @InterfaceAudience.Public
    public boolean runInTransaction(TransactionalTask transactionalTask) {

        boolean shouldCommit = true;

        beginTransaction();
        try {
            shouldCommit = transactionalTask.run();
        } catch (Exception e) {
            shouldCommit = false;
            Log.e(Database.TAG, e.toString(), e);
            throw new RuntimeException(e);
        } finally {
            endTransaction(shouldCommit);
        }

        return shouldCommit;
    }

    /**
     * Runs the delegate asynchronously.
     */
    @InterfaceAudience.Public
    public Future runAsync(final AsyncTask asyncTask) {
        return getManager().runAsync(new Runnable() {
            @Override
            public void run() {
                asyncTask.run(Database.this);
            }
        });
    }

    /**
     * Creates a new Replication that will push to the target Database at the given url.
     *
     * @param remote the remote URL to push to
     * @return A new Replication that will push to the target Database at the given url.
     */
    @InterfaceAudience.Public
    public Replication createPushReplication(URL remote) {
        return new Replication(this, remote, Replication.Direction.PUSH, null, manager.getWorkExecutor());
    }

    /**
     * Creates a new Replication that will pull from the source Database at the given url.
     *
     * @param remote the remote URL to pull from
     * @return A new Replication that will pull from the source Database at the given url.
     */
    @InterfaceAudience.Public
    public Replication createPullReplication(URL remote) {
        return new Replication(this, remote, Replication.Direction.PULL, null, manager.getWorkExecutor());

    }

    /**
     * Adds a Database change delegate that will be called whenever a Document within the Database changes.
     * @param listener
     */
    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener listener) {
        changeListeners.addIfAbsent(listener);
    }

    /**
     * Removes the specified delegate as a listener for the Database change event.
     * @param listener
     */
    @InterfaceAudience.Public
    public void removeChangeListener(ChangeListener listener) {
        changeListeners.remove(listener);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void addDatabaseListener(DatabaseListener listener) {
        databaseListeners.addIfAbsent(listener);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void removeDatabaseListener(DatabaseListener listener) {
        databaseListeners.remove(listener);
    }

    /**
     * Returns a string representation of this database.
     */
    @InterfaceAudience.Public
    public String toString() {
        return this.getClass().getName() + "[" + path + "]";
    }

    /**
     * The type of event raised when a Database changes.
     */
    @InterfaceAudience.Public
    public static class ChangeEvent {

        private Database source;
        private boolean isExternal;
        private List<DocumentChange> changes;

        public ChangeEvent(Database source, boolean isExternal, List<DocumentChange> changes) {
            this.source = source;
            this.isExternal = isExternal;
            this.changes = changes;
        }

        public Database getSource() {
            return source;
        }

        public boolean isExternal() {
            return isExternal;
        }

        public List<DocumentChange> getChanges() {
            return changes;
        }

    }

    /**
     * A delegate that can be used to listen for Database changes.
     */
    @InterfaceAudience.Public
    public static interface ChangeListener {
        public void changed(ChangeEvent event);
    }


    @InterfaceAudience.Private
    public static interface DatabaseListener {
        // CBL_DatabaseWillCloseNotification
        public void databaseClosing();
    }

    /**
     * Get the maximum depth of a document's revision tree (or, max length of its revision history.)
     * Revisions older than this limit will be deleted during a -compact: operation.
     * Smaller values save space, at the expense of making document conflicts somewhat more likely.
     */
    @InterfaceAudience.Public
    public int getMaxRevTreeDepth() {
        return maxRevTreeDepth;
    }

    /**
     * Set the maximum depth of a document's revision tree (or, max length of its revision history.)
     * Revisions older than this limit will be deleted during a -compact: operation.
     * Smaller values save space, at the expense of making document conflicts somewhat more likely.
     */
    @InterfaceAudience.Public
    public void setMaxRevTreeDepth(int maxRevTreeDepth) {
        this.maxRevTreeDepth = maxRevTreeDepth;
    }


    /** PRIVATE METHODS **/

    /**
     * Returns the already-instantiated cached Document with the given ID, or nil if none is yet cached.
     * @exclude
     */
    @InterfaceAudience.Private
    protected Document getCachedDocument(String documentID) {
        return docCache.get(documentID);
    }

    /**
     * Empties the cache of recently used Document objects.
     * API calls will now instantiate and return new instances.
     * @exclude
     */
    @InterfaceAudience.Private
    public void clearDocumentCache() {
        docCache.clear();
    }

    /**
     * Get all the active replicators associated with this database.
     */
    @InterfaceAudience.Private
    public List<Replication> getActiveReplications() {
        List<Replication> activeReplicatorsList =  new ArrayList<Replication>();
        if (activeReplicators != null) {
            activeReplicatorsList.addAll(activeReplicators);
        }
        return activeReplicatorsList;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected void removeDocumentFromCache(Document document) {
        docCache.remove(document.getId());
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public boolean exists() {
        return new File(path).exists();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getAttachmentStorePath() {
        String attachmentStorePath = path;
        int lastDotPosition = attachmentStorePath.lastIndexOf('.');
        if( lastDotPosition > 0 ) {
            attachmentStorePath = attachmentStorePath.substring(0, lastDotPosition);
        }
        attachmentStorePath = attachmentStorePath + " attachments";
        return attachmentStorePath;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private String getObsoletedAttachmentStorePath() {
        String attachmentStorePath = path;
        int lastDotPosition = attachmentStorePath.lastIndexOf('.');
        if( lastDotPosition > 0 ) {
            attachmentStorePath = attachmentStorePath.substring(0, lastDotPosition);
        }
        attachmentStorePath = attachmentStorePath + File.separator + "attachments";
        return attachmentStorePath;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private String getObsoletedAttachmentStoreParentPath() {
        String attachmentStorePath = path;
        int lastDotPosition = attachmentStorePath.lastIndexOf('.');
        if( lastDotPosition > 0 ) {
            attachmentStorePath = attachmentStorePath.substring(0, lastDotPosition);
        }
        return attachmentStorePath;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static Database createEmptyDBAtPath(String path, Manager manager) {
        if(!FileDirUtils.removeItemIfExists(path)) {
            return null;
        }
        Database result = new Database(path, manager);
        File af = new File(result.getAttachmentStorePath());
        //recursively delete attachments path
        if(!FileDirUtils.deleteRecursive(af)) {
            return null;
        }
        if(!result.open()) {
            return null;
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public boolean initialize(String statements) {
        try {
            for (String statement : statements.split(";")) {
                database.execSQL(statement);
            }
        } catch (SQLException e) {
            close();
            return false;
        }
        return true;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public synchronized boolean open() {

        if (open) {
            return true;
        }

        // Create the storage engine.
        SQLiteStorageEngineFactory sqliteStorageEngineFactoryDefault = manager.getContext().getSQLiteStorageEngineFactory();
        database = sqliteStorageEngineFactoryDefault.createStorageEngine();

        // Try to open the storage engine and stop if we fail.
        if (database == null || !database.open(path)) {
            String msg = "Unable to create a storage engine, fatal error";
            Log.e(Database.TAG, msg);
            throw new IllegalStateException(msg);
        }

        // Stuff we need to initialize every time the sqliteDb opens:
        if (!initialize("PRAGMA foreign_keys = ON;")) {
            Log.e(Database.TAG, "Error turning on foreign keys");
            return false;
        }

        // Check the user_version number we last stored in the sqliteDb:
        int dbVersion = database.getVersion();

        // Incompatible version changes increment the hundreds' place:
        if (dbVersion >= 200) {
            Log.e(Database.TAG, "Database: Database version (%d) is newer than I know how to work with", dbVersion);
            database.close();
            return false;
        }

        boolean isNew = (dbVersion == 0);
        boolean isSuccessful = false;

        // BEGIN TRANSACTION
        if (!beginTransaction()) {
            database.close();
            return false;
        }

        try {
            if (dbVersion < 1) {
                // First-time initialization:
                // (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
                // monotonically increasing, never reused. See <http://www.sqlite.org/autoinc.html>)
                if (!initialize(SCHEMA)) {
                    return false;
                }
                dbVersion = 3;
            }

            if (dbVersion < 2) {
                // Version 2: added attachments.revpos
                String upgradeSql = "ALTER TABLE attachments ADD COLUMN revpos INTEGER DEFAULT 0; " +
                        "PRAGMA user_version = 2";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 2;
            }

            if (dbVersion < 3) {
                String upgradeSql = "CREATE TABLE IF NOT EXISTS localdocs ( " +
                        "docid TEXT UNIQUE NOT NULL, " +
                        "revid TEXT NOT NULL, " +
                        "json BLOB); " +
                        "CREATE INDEX IF NOT EXISTS localdocs_by_docid ON localdocs(docid); " +
                        "PRAGMA user_version = 3";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 3;
            }

            if (dbVersion < 4) {
                String upgradeSql = "CREATE TABLE info ( " +
                        "key TEXT PRIMARY KEY, " +
                        "value TEXT); " +
                        "INSERT INTO INFO (key, value) VALUES ('privateUUID', '" + Misc.TDCreateUUID() + "'); " +
                        "INSERT INTO INFO (key, value) VALUES ('publicUUID',  '" + Misc.TDCreateUUID() + "'); " +
                        "PRAGMA user_version = 4";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 4;
            }

            if (dbVersion < 5) {
                // Version 5: added encoding for attachments
                String upgradeSql = "ALTER TABLE attachments ADD COLUMN encoding INTEGER DEFAULT 0; " +
                        "ALTER TABLE attachments ADD COLUMN encoded_length INTEGER; " +
                        "PRAGMA user_version = 5";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 5;
            }


            if (dbVersion < 6) {
                // Version 6: enable Write-Ahead Log (WAL) <http://sqlite.org/wal.html>
                // Not supported on Android, require SQLite 3.7.0
                //String upgradeSql  = "PRAGMA journal_mode=WAL; " +

                // NOTE: For Android, WAL should be set when open the database
                //       Check AndroidSQLiteStorageEngine.java public boolean open(String path) method.
                //       http://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#enableWriteAheadLogging()

                String upgradeSql = "PRAGMA user_version = 6";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 6;
            }

            if (dbVersion < 7) {
                // Version 7: enable full-text search
                // Note: Apple's SQLite build does not support the icu or unicode61 tokenizers :(
                // OPT: Could add compress/decompress functions to make stored content smaller
                // Not supported on Android
                //String upgradeSql = "CREATE VIRTUAL TABLE fulltext USING fts4(content, tokenize=unicodesn); " +
                //"ALTER TABLE maps ADD COLUMN fulltext_id INTEGER; " +
                //"CREATE INDEX IF NOT EXISTS maps_by_fulltext ON maps(fulltext_id); " +
                //"CREATE TRIGGER del_fulltext DELETE ON maps WHEN old.fulltext_id not null " +
                //"BEGIN DELETE FROM fulltext WHERE rowid=old.fulltext_id| END; " +
                String upgradeSql = "PRAGMA user_version = 7";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 7;
            }

            // (Version 8 was an older version of the geo index)

            if (dbVersion < 9) {
                // Version 9: Add geo-query index
                //String upgradeSql = "CREATE VIRTUAL TABLE bboxes USING rtree(rowid, x0, x1, y0, y1); " +
                //"ALTER TABLE maps ADD COLUMN bbox_id INTEGER; " +
                //"ALTER TABLE maps ADD COLUMN geokey BLOB; " +
                //"CREATE TRIGGER del_bbox DELETE ON maps WHEN old.bbox_id not null " +
                //"BEGIN DELETE FROM bboxes WHERE rowid=old.bbox_id| END; " +
                String upgradeSql = "PRAGMA user_version = 9";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 9;
            }

            if (dbVersion < 10) {
                // Version 10: Add rev flag for whether it has an attachment
                String upgradeSql = "ALTER TABLE revs ADD COLUMN no_attachments BOOLEAN; " +
                        "PRAGMA user_version = 10";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 10;
            }

            // (Version 11 used to create the index revs_cur_deleted, which is obsoleted in version 16)

            // (Version 12: CBL Android/Java skipped 12 before. Instead, we ported bug fix at dbVersion 18)

            if (dbVersion < 13) {
                // Version 13: Add rows to track number of rows in the views
                String upgradeSql = "ALTER TABLE views ADD COLUMN total_docs INTEGER DEFAULT -1; " +
                        "PRAGMA user_version = 13";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 13;
            }

            if (dbVersion < 14) {
                // Version 14: Add index for getting a document with doc and rev id
                String upgradeSql = "CREATE INDEX IF NOT EXISTS revs_by_docid_revid ON revs(doc_id, revid desc, current, deleted); " +
                        "PRAGMA user_version = 14";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 14;
            }

            if (dbVersion < 15) {
                // Version 15: Add sequence index on maps and attachments for revs(sequence) on DELETE CASCADE
                String upgradeSql = "CREATE INDEX maps_sequence ON maps(sequence); " +
                        "CREATE INDEX attachments_sequence ON attachments(sequence); " +
                        "PRAGMA user_version = 15";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 15;
            }


            if (dbVersion < 16) {
                // Version 16: Fix the very suboptimal index revs_cur_deleted.
                // The new revs_current is an optimal index for finding the winning revision of a doc.
                String upgradeSql = "DROP INDEX IF EXISTS revs_current; " +
                        "DROP INDEX IF EXISTS revs_cur_deleted; " +
                        "CREATE INDEX revs_current ON revs(doc_id, current desc, deleted, revid desc);" +
                        "PRAGMA user_version = 16";

                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 16;
            }

            if (dbVersion < 17) {
                // Version 17: https://github.com/couchbase/couchbase-lite-ios/issues/615
                String upgradeSql = "CREATE INDEX maps_view_sequence ON maps(view_id, sequence); " +
                        "PRAGMA user_version = 17";

                if (!initialize(upgradeSql)) {
                    database.close();
                    return false;
                }
                dbVersion = 17;
            }

            // Note: We skipped change for dbVersion 12 before. 18 is for JSONCollator bug fix.
            //       Android version should be one version higher.
            if (dbVersion < 18) {
                // Version 12: Because of a bug fix that changes JSON collation, invalidate view indexes

                // instead of delete all rows in maps table, drop table and recreate it.
                String upgradeSql = "DROP TABLE maps";
                if (!initialize(upgradeSql)) {
                    return false;
                }

                upgradeSql = "CREATE TABLE IF NOT EXISTS maps ( " +
                    " view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, " +
                    " sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
                    " key TEXT NOT NULL COLLATE JSON, " +
                    " value TEXT); " +
                    " CREATE INDEX IF NOT EXISTS maps_keys on maps(view_id, key COLLATE JSON); " +
                    " CREATE INDEX IF NOT EXISTS maps_sequence ON maps(sequence);";
                if (!initialize(upgradeSql)) {
                    return false;
                }

                upgradeSql = "UPDATE views SET lastsequence=0; " +
                        "PRAGMA user_version = 18";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 18;
            }

            // NOTE: Following lines of code are for compatibility with Couchbase Lite iOS v1.1.0 database format.
            //       https://github.com/couchbase/couchbase-lite-java-core/issues/596
            //       CBL iOS v1.1.0 => 101
            //       1. Creates attachments table if it does not exist.
            //       2. Iterate revs table to populate attachments table.
            if (dbVersion >= 101) {

                // NOTE: CBL iOS v1.1.0 does not have maps table, Needs to create it if it does not exist.
                String upgradeSql = "CREATE TABLE IF NOT EXISTS maps ( " +
                        " view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, " +
                        " sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
                        " key TEXT NOT NULL COLLATE JSON, " +
                        " value TEXT); " +
                        " CREATE INDEX IF NOT EXISTS maps_keys on maps(view_id, key COLLATE JSON); " +
                        " CREATE INDEX IF NOT EXISTS maps_sequence ON maps(sequence);";
                if (!initialize(upgradeSql)) {
                    return false;
                }


                // Check if attachments table exists. If not, create the table, and iterate revs
                // to populate attachment table
                boolean existsAttachments = false;
                Cursor cursor = null;
                try {
                    cursor = database.rawQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='attachments'", null);
                    if (cursor.moveToNext()) {
                        existsAttachments = true;
                    }
                } catch (SQLException e) {
                    Log.e(Database.TAG, "Failed to check if attachments table exists", e);
                    return false;
                } finally {
                    if (cursor != null) {
                        cursor.close();
                    }
                }

                if (!existsAttachments) {
                    // 1. create attachments table
                    upgradeSql = "CREATE TABLE attachments ( " +
                            "sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
                            "filename TEXT NOT NULL, " +
                            "key BLOB NOT NULL, " +
                            "type TEXT, " +
                            "length INTEGER NOT NULL, " +
                            "encoding INTEGER DEFAULT 0, " +
                            "encoded_length INTEGER, " +
                            "revpos INTEGER DEFAULT 0); " +
                            "CREATE INDEX attachments_by_sequence on attachments(sequence, filename); " +
                            "CREATE INDEX attachments_sequence ON attachments(sequence); " +
                            "PRAGMA user_version = 20";
                    if (!initialize(upgradeSql)) {
                        return false;
                    }

                    // 2. iterate revs table, and populate attachment table
                    String sql = "SELECT sequence, json FROM revs WHERE no_attachments=0";
                    Cursor cursor2 = null;
                    try {
                        cursor2 = database.rawQuery(sql, null);
                        while (cursor2.moveToNext()) {
                            if (!cursor2.isNull(1)) {
                                long sequence = cursor2.getLong(0);
                                byte[] json = cursor2.getBlob(1);
                                try {
                                    Map<String, Object> docProperties = Manager.getObjectMapper().readValue(json, Map.class);
                                    Map<String, Object> attachments = (Map<String, Object>) docProperties.get("_attachments");
                                    Iterator<String> itr = attachments.keySet().iterator();
                                    while (itr.hasNext()) {
                                        String name = itr.next();
                                        Map<String, Object> attachment = (Map<String, Object>) attachments.get(name);
                                        String contentType = (String) attachment.get("content_type");
                                        int revPos = (Integer) attachment.get("revpos");
                                        int length = (Integer) attachment.get("length");
                                        String digest = (String) attachment.get("digest");
                                        int encodedLength = -1;
                                        if(attachment.containsKey("encoded_length"))
                                            encodedLength = (Integer)attachment.get("encoded_length");
                                        AttachmentInternal.AttachmentEncoding encoding = AttachmentInternal.AttachmentEncoding.AttachmentEncodingNone;
                                        if(attachment.containsKey("encoding") && attachment.get("encoding").equals("gzip")){
                                            encoding = AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP;
                                        }
                                        BlobKey key = new BlobKey(digest);
                                        try {
                                            insertAttachmentForSequenceWithNameAndType(sequence, name, contentType, revPos, key, length, encoding, encodedLength);
                                        } catch (CouchbaseLiteException e) {
                                            Log.e(Log.TAG_DATABASE, "Attachment information inserstion error: " + name + "=" + attachment.toString(), e);
                                            return false;
                                        }
                                    }
                                } catch (Exception e) {
                                    Log.e(Log.TAG_DATABASE, "JSON parsing error: " + new String(json), e);
                                    return false;
                                }
                            }
                        }
                    } catch (SQLException e) {
                        Log.e(Database.TAG, "Failed to check if attachments table exists", e);
                        return false;
                    } finally {
                        if (cursor2 != null) {
                            cursor2.close();
                        }
                    }
                }
                dbVersion = 20;
            }

            // NOTE: CBL Android/Java v1.1.0 Set database version 20.
            //       20 is higher than any previous release, but lower than CBL iOS v1.1.0 - 101
            if (dbVersion < 20) {
                String upgradeSql = "PRAGMA user_version = 20";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 20;
            }

            if (isNew) {
                optimizeSQLIndexes(); // runs ANALYZE query
            }

            // successfully updated database schema
            isSuccessful = true;

        } finally {
            // END TRANSACTION WITH COMMIT OR ROLLBACK
            endTransaction(isSuccessful);

            // if failed, close database before return
            if (!isSuccessful) {
                database.close();
            }
        }

        // NOTE: Migrate attachment directory path if necessary
        // https://github.com/couchbase/couchbase-lite-java-core/issues/604
        File obsoletedAttachmentStorePath = new File(getObsoletedAttachmentStorePath());
        if (obsoletedAttachmentStorePath != null && obsoletedAttachmentStorePath.exists() && obsoletedAttachmentStorePath.isDirectory()) {
            File attachmentStorePath = new File(getAttachmentStorePath());
            if (attachmentStorePath != null && !attachmentStorePath.exists()) {
                boolean success = obsoletedAttachmentStorePath.renameTo(attachmentStorePath);
                if (!success) {
                    Log.e(Database.TAG, "Could not rename attachment store path");
                    database.close();
                    return false;
                }
            }
        }

        // NOTE: obsoleted directory is /files/<database name>/attachments/xxxx
        //       Needs to delete /files/<database name>/ too
        File obsoletedAttachmentStoreParentPath = new File(getObsoletedAttachmentStoreParentPath());
        if (obsoletedAttachmentStoreParentPath != null && obsoletedAttachmentStoreParentPath.exists()) {
            obsoletedAttachmentStoreParentPath.delete();
        }

        try {
            if (isBlobstoreMigrated() || !manager.isAutoMigrateBlobStoreFilename()) {
                attachments = new BlobStore(getAttachmentStorePath(), false);
            } else {
                attachments = new BlobStore(getAttachmentStorePath(), true);
                markBlobstoreMigrated();
            }

        } catch (IllegalArgumentException e) {
            Log.e(Database.TAG, "Could not initialize attachment store", e);
            database.close();
            return false;
        }

        open = true;
        return true;
    }

    private boolean isBlobstoreMigrated() {
        Map<String, Object> props = getExistingLocalDocument("_blobstore");
        if (props != null && props.containsKey("blobstoreMigrated"))
            return (Boolean) props.get("blobstoreMigrated");
        return false;
    }

    private void markBlobstoreMigrated() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("blobstoreMigrated", true);
        try {
            putLocalDocument("_blobstore", props);
        } catch (CouchbaseLiteException e) {
            Log.e(Log.TAG_DATABASE, e.getMessage());
        }
    }

    @InterfaceAudience.Public
    public boolean close() {
        if (!open) {
            return false;
        }

        for(DatabaseListener listener : databaseListeners){
            listener.databaseClosing();
        }

        if (views != null) {
            for (View view : views.values()) {
                view.databaseClosing();
            }
        }
        views = null;

        if (activeReplicators != null) {
            List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
            synchronized (activeReplicators) {
                for (Replication replicator : activeReplicators) {
                    // handler to check if the replicator stopped
                    final CountDownLatch latch = new CountDownLatch(1);
                    replicator.addChangeListener(new Replication.ChangeListener() {
                        @Override
                        public void changed(Replication.ChangeEvent event) {
                            if (event.getSource().getStatus() == Replication.ReplicationStatus.REPLICATION_STOPPED) {
                                latch.countDown();
                            }
                        }
                    });
                    latches.add(latch);

                    // ask replicator to stop
                    replicator.databaseClosing();
                }
            }

            // wait till all replicator stopped
            for (CountDownLatch latch : latches) {
                try {
                    boolean success = latch.await(Replication.DEFAULT_MAX_TIMEOUT_FOR_SHUTDOWN, TimeUnit.SECONDS);
                    if (!success) {
                        Log.w(Log.TAG_DATABASE, "Replicator could not stop in " + Replication.DEFAULT_MAX_TIMEOUT_FOR_SHUTDOWN + " second.");
                    }
                } catch (Exception e) {
                    Log.w(Log.TAG_DATABASE, e.getMessage());
                }
            }

            activeReplicators = null;
        }

        allReplicators = null;

        if (database != null && database.isOpen()) {
            database.close();
        }
        open = false;
        return true;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getPath() {
        return path;
    }


    // Leave this package protected, so it can only be used
    // View uses this accessor

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    SQLiteStorageEngine getDatabase() {
        return database;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public BlobStore getAttachments() {
        return attachments;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public BlobStoreWriter getAttachmentWriter() {
        return new BlobStoreWriter(getAttachments());
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public long totalDataSize() {
        File f = new File(path);
        long size = f.length() + attachments.totalDataSize();
        return size;
    }

    /**
     * Begins a database transaction. Transactions can nest.
     * Every beginTransaction() must be balanced by a later endTransaction()
     *
     * Notes: 1. SQLiteDatabase.beginTransaction() supported nested transaction. But, in case
     *           nested transaction rollbacked, it also rollbacks outer transaction too.
     *           This is not ideal behavior for CBL
     *        2. SAVEPOINT...RELEASE supports nested transaction. But Android API 14 and 15,
     *           it throws Exception. I assume it is Android bug. As CBL need to support from API 10
     *           . So it does not work for CBL.
     *        3. Using Transaction for outer/1st level of transaction and inner/2nd level of transaction
     *           works with CBL requirement.
     *        4. CBL Android and Java uses Thread, So it is better to use SQLiteDatabase.beginTransaction()
     *           for outer/1st level transaction. if we use BEGIN...COMMIT and SAVEPOINT...RELEASE,
     *           we need to implement wrapper of BEGIN...COMMIT and SAVEPOINT...RELEASE to be
     *           Thread-safe.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public boolean beginTransaction() {
        int tLevel = transactionLevel.get();
        //Log.v(Log.TAG_DATABASE, "[Database.beginTransaction()] Database=" + this + ", SQLiteStorageEngine=" + database + ", transactionLevel=" + transactionLevel + ", currentThread=" + Thread.currentThread());
        try {
            // Outer (level 0)  transaction. Use SQLiteDatabase.beginTransaction()
            if (tLevel == 0) {
                database.beginTransaction();
            }
            // Inner (level 1 or higher) transaction. Use SQLite's SAVEPOINT
            else {
                database.execSQL("SAVEPOINT cbl_" + Integer.toString(tLevel));
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
     * @param commit If true, commits; if false, aborts and rolls back, undoing all changes made since the matching -beginTransaction call, *including* any committed nested transactions.
     * @exclude
     */
    @InterfaceAudience.Private
    public boolean endTransaction(boolean commit) {
        int tLevel = transactionLevel.get();

        assert (tLevel > 0);

        transactionLevel.set(--tLevel);

        // Outer (level 0) transaction. Use SQLiteDatabase.setTransactionSuccessful() and SQLiteDatabase.endTransaction()
        if (tLevel == 0) {
            if (commit) {
                Log.v(Log.TAG_DATABASE, "%s Committing transaction (level %d)", Thread.currentThread().getName(), tLevel);
                database.setTransactionSuccessful();
                database.endTransaction();
            } else {
                Log.v(Log.TAG_DATABASE, "%s CANCEL transaction (level %d)", Thread.currentThread().getName(), tLevel);
                try {
                    database.endTransaction();
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
                    database.execSQL(";ROLLBACK TO cbl_" + Integer.toString(tLevel));
                } catch (SQLException e) {
                    Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling endTransaction()", e);
                    return false;
                }
            }
            try {
                database.execSQL("RELEASE cbl_" + Integer.toString(tLevel));
            } catch (SQLException e) {
                Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling endTransaction()", e);
                return false;
            }
        }

        storageExitedTransaction(commit);

        return true;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String privateUUID() {
        String result = null;
        Cursor cursor = null;
        try {
            cursor = database.rawQuery("SELECT value FROM info WHERE key='privateUUID'", null);
            if(cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } catch(SQLException e) {
            Log.e(TAG, "Error querying privateUUID", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String publicUUID() {
        String result = null;
        Cursor cursor = null;
        try {
            cursor = database.rawQuery("SELECT value FROM info WHERE key='publicUUID'", null);
            if(cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } catch(SQLException e) {
            Log.e(TAG, "Error querying privateUUID", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String infoForKey(String key) {
        String result = null;
        Cursor cursor = null;
        try {
            String[] args = {key};
            cursor = database.rawQuery("SELECT value FROM info WHERE key=?", args);
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

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public long setInfo(String key, String info) {
        long result = 0;
        try {
            ContentValues args = new ContentValues();
            args.put("key", key);
            args.put("value", info);
            result = database.insertWithOnConflict("info", null, args, SQLiteStorageEngine.CONFLICT_REPLACE);

        } catch (Exception e) {
            Log.e(Database.TAG, "Error inserting document id", e);
        }
        return result;
    }

    /** GETTING DOCUMENTS: **/


    /**
     * Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.
     * @exclude
     */
    @InterfaceAudience.Private
    public  byte[] appendDictToJSON(byte[] json, Map<String,Object> dict) {
        if(dict.size() == 0) {
            return json;
        }

        byte[] extraJSON = null;
        try {
            extraJSON = Manager.getObjectMapper().writeValueAsBytes(dict);
        } catch (Exception e) {
            Log.e(Database.TAG, "Error convert extra JSON to bytes", e);
            return null;
        }

        int jsonLength = json.length;
        int extraLength = extraJSON.length;
        if(jsonLength == 2) { // Original JSON was empty
            return extraJSON;
        }
        byte[] newJson = new byte[jsonLength + extraLength - 1];
        System.arraycopy(json, 0, newJson, 0, jsonLength - 1);  // Copy json w/o trailing '}'
        newJson[jsonLength - 1] = ',';  // Add a ','
        System.arraycopy(extraJSON, 1, newJson, jsonLength, extraLength - 1);
        return newJson;
    }

    /**
     * Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
     * Rev must already have its revID and sequence properties set.
     * @exclude
     */
    @InterfaceAudience.Private
    public Map<String,Object> extraPropertiesForRevision(RevisionInternal rev, EnumSet<TDContentOptions> contentOptions) {

        String docId = rev.getDocId();
        String revId = rev.getRevId();
        long sequenceNumber = rev.getSequence();
        assert(revId != null);
        assert(sequenceNumber > 0);

        Map<String, Object> attachmentsDict = null;
        // Get attachment metadata, and optionally the contents:
        if (!contentOptions.contains(TDContentOptions.TDNoAttachments)) {
            attachmentsDict = getAttachmentsDictForSequenceWithContent(sequenceNumber, contentOptions);
        }

        // Get more optional stuff to put in the properties:
        //OPT: This probably ends up making redundant SQL queries if multiple options are enabled.
        Long localSeq = null;
        if(contentOptions.contains(TDContentOptions.TDIncludeLocalSeq)) {
            localSeq = sequenceNumber;
        }

        Map<String,Object> revHistory = null;
        if(contentOptions.contains(TDContentOptions.TDIncludeRevs)) {
            revHistory = getRevisionHistoryDict(rev);
        }

        List<Object> revsInfo = null;
        if(contentOptions.contains(TDContentOptions.TDIncludeRevsInfo)) {
            revsInfo = new ArrayList<Object>();
            List<RevisionInternal> revHistoryFull = getRevisionHistory(rev);
            for (RevisionInternal historicalRev : revHistoryFull) {
                Map<String,Object> revHistoryItem = new HashMap<String,Object>();
                String status = "available";
                if(historicalRev.isDeleted()) {
                    status = "deleted";
                }
                if (historicalRev.isMissing()) {
                    status = "missing";
                }
                revHistoryItem.put("rev", historicalRev.getRevId());
                revHistoryItem.put("status", status);
                revsInfo.add(revHistoryItem);
            }
        }

        List<String> conflicts = null;
        if(contentOptions.contains(TDContentOptions.TDIncludeConflicts)) {
            RevisionList revs = getAllRevisionsOfDocumentID(docId, true);
            if(revs.size() > 1) {
                conflicts = new ArrayList<String>();
                for (RevisionInternal aRev : revs) {
                    if(aRev.equals(rev) || aRev.isDeleted()) {
                        // don't add in this case
                    } else {
                        conflicts.add(aRev.getRevId());
                    }
                }
            }
        }

        Map<String,Object> result = new HashMap<String,Object>();
        result.put("_id", docId);
        result.put("_rev", revId);
        if(rev.isDeleted()) {
            result.put("_deleted", true);
        }
        if(attachmentsDict != null) {
            result.put("_attachments", attachmentsDict);
        }
        if(localSeq != null) {
            result.put("_local_seq", localSeq);
        }
        if(revHistory != null) {
            result.put("_revisions", revHistory);
        }
        if(revsInfo != null) {
            result.put("_revs_info", revsInfo);
        }
        if(conflicts != null) {
            result.put("_conflicts", conflicts);
        }

        return result;
    }

    /**
     * Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
     * Rev must already have its revID and sequence properties set.
     * @exclude
     */
    @InterfaceAudience.Private
    public void expandStoredJSONIntoRevisionWithAttachments(byte[] json, RevisionInternal rev, EnumSet<TDContentOptions> contentOptions) {
        Map<String,Object> extra = extraPropertiesForRevision(rev, contentOptions);
        if(json != null && json.length > 0) {
            rev.setJson(appendDictToJSON(json, extra));
        }
        else {
            rev.setProperties(extra);
            if (json == null)
                rev.setMissing(true);
        }
    }

    /**
     * @exclude
     */
    @SuppressWarnings("unchecked")
    @InterfaceAudience.Private
    public Map<String, Object> documentPropertiesFromJSON(byte[] json, String docId, String revId, boolean deleted, long sequence, EnumSet<TDContentOptions> contentOptions) {

        RevisionInternal rev = new RevisionInternal(docId, revId, deleted);
        rev.setSequence(sequence);
        Map<String, Object> extra = extraPropertiesForRevision(rev, contentOptions);
        if (json == null) {
            return extra;
        }

        Map<String, Object> docProperties = null;
        try {
            docProperties = Manager.getObjectMapper().readValue(json, Map.class);
            docProperties.putAll(extra);
            return docProperties;
        } catch (Exception e) {
            Log.e(Database.TAG, "Error serializing properties to JSON", e);
        }

        return docProperties;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionInternal getDocumentWithIDAndRev(String id, String rev, EnumSet<TDContentOptions> contentOptions) {
        RevisionInternal result = null;
        String sql;

        Cursor cursor = null;
        try {
            cursor = null;
            String cols = "revid, deleted, sequence, no_attachments";
            if(!contentOptions.contains(TDContentOptions.TDNoBody)) {
                cols += ", json";
            }
            if(rev != null) {
                sql = "SELECT " + cols + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? LIMIT 1";
                //TODO: mismatch w iOS: {sql = "SELECT " + cols + " FROM revs WHERE revs.doc_id=? AND revid=? AND json notnull LIMIT 1";}
                String[] args = {id, rev};
                cursor = database.rawQuery(sql, args);
            }
            else {
                sql = "SELECT " + cols + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id and current=1 and deleted=0 ORDER BY revid DESC LIMIT 1";
                //TODO: mismatch w iOS: {sql = "SELECT " + cols + " FROM revs WHERE revs.doc_id=? and current=1 and deleted=0 ORDER BY revid DESC LIMIT 1";}
                String[] args = {id};
                cursor = database.rawQuery(sql, args);
            }

            if(cursor.moveToNext()) {
                if(rev == null) {
                    rev = cursor.getString(0);
                }
                boolean deleted = (cursor.getInt(1) > 0);
                result = new RevisionInternal(id, rev, deleted);
                result.setSequence(cursor.getLong(2));
                if(!contentOptions.equals(EnumSet.of(TDContentOptions.TDNoBody))) {
                    byte[] json = null;
                    if(!contentOptions.contains(TDContentOptions.TDNoBody)) {
                        json = cursor.getBlob(4);
                    }
                    if (cursor.getInt(3) > 0) // no_attachments == true
                        contentOptions.add(TDContentOptions.TDNoAttachments);
                    expandStoredJSONIntoRevisionWithAttachments(json, result, contentOptions);
                }
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting document with id and rev", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public boolean existsDocumentWithIDAndRev(String docId, String revId) {
        return getDocumentWithIDAndRev(docId, revId, EnumSet.of(TDContentOptions.TDNoBody)) != null;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionInternal loadRevisionBody(RevisionInternal rev, EnumSet<TDContentOptions> contentOptions) throws CouchbaseLiteException {
        if (rev.getBody() != null && contentOptions == EnumSet.noneOf(TDContentOptions.class) && rev.getSequence() != 0) {
            return rev;
        }

        if ((rev.getDocId() == null) || (rev.getRevId() == null)) {
            Log.e(Database.TAG, "Error loading revision body");
            throw new CouchbaseLiteException(Status.PRECONDITION_FAILED);
        }

        Cursor cursor = null;
        Status result = new Status(Status.NOT_FOUND);
        try {
            // TODO: on ios this query is:
            // TODO: "SELECT sequence, json FROM revs WHERE doc_id=? AND revid=? LIMIT 1"
            String sql = "SELECT sequence, json FROM revs, docs WHERE revid=? AND docs.docid=? AND revs.doc_id=docs.doc_id LIMIT 1";
            String[] args = {rev.getRevId(), rev.getDocId()};
            cursor = database.rawQuery(sql, args);
            if (cursor.moveToNext()) {
                result.setCode(Status.OK);
                rev.setSequence(cursor.getLong(0));
                expandStoredJSONIntoRevisionWithAttachments(cursor.getBlob(1), rev, contentOptions);
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error loading revision body", e);
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

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public long getDocNumericID(String docId) {
        Cursor cursor = null;
        String[] args = { docId };

        long result = -1;
        try {
            cursor = database.rawQuery("SELECT doc_id FROM docs WHERE docid=?", args);

            if(cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
            else {
                result = 0;
            }
        } catch (Exception e) {
            Log.e(Database.TAG, "Error getting doc numeric id", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /** HISTORY: **/

    /**
     * Returns all the known revisions (or all current/conflicting revisions) of a document.
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionList getAllRevisionsOfDocumentID(String docId, long docNumericID, boolean onlyCurrent) {

        String sql = null;
        if(onlyCurrent) {
            sql = "SELECT sequence, revid, deleted FROM revs " +
                    "WHERE doc_id=? AND current ORDER BY sequence DESC";
        }
        else {
            sql = "SELECT sequence, revid, deleted FROM revs " +
                    "WHERE doc_id=? ORDER BY sequence DESC";
        }

        String[] args = { Long.toString(docNumericID) };
        Cursor cursor = null;

        cursor = database.rawQuery(sql, args);

        RevisionList result;
        try {
            cursor.moveToNext();
            result = new RevisionList();
            while(!cursor.isAfterLast()) {
                RevisionInternal rev = new RevisionInternal(docId, cursor.getString(1), (cursor.getInt(2) > 0));
                rev.setSequence(cursor.getLong(0));
                result.add(rev);
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting all revisions of document", e);
            return null;
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionList getAllRevisionsOfDocumentID(String docId, boolean onlyCurrent) {
        long docNumericId = getDocNumericID(docId);
        if(docNumericId < 0) {
            return null;
        }
        else if(docNumericId == 0) {
            return new RevisionList();
        }
        else {
            return getAllRevisionsOfDocumentID(docId, docNumericId, onlyCurrent);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public List<String> getConflictingRevisionIDsOfDocID(String docID) {
        long docIdNumeric = getDocNumericID(docID);
        if(docIdNumeric < 0) {
            return null;
        }

        List<String> result = new ArrayList<String>();
        Cursor cursor = null;
        try {
            String[] args = { Long.toString(docIdNumeric) };
            cursor = database.rawQuery("SELECT revid FROM revs WHERE doc_id=? AND current " +
                                           "ORDER BY revid DESC OFFSET 1", args);
            cursor.moveToNext();
            while(!cursor.isAfterLast()) {
                result.add(cursor.getString(0));
                cursor.moveToNext();
            }

        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting all revisions of document", e);
            return null;
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public List<String>  getPossibleAncestorRevisionIDs( RevisionInternal rev, int limit, AtomicBoolean hasAttachment )
    {
        int generation = rev.getGeneration();
        if (generation <= 1)
            return null;

        long docNumericID = getDocNumericID(rev.getDocId());
        if (docNumericID <= 0)
            return null;

        List<String> revIDs = new ArrayList<String>();

        int sqlLimit = limit > 0 ? (int)limit : -1;     // SQL uses -1, not 0, to denote 'no limit'
        String sql = "SELECT revid, sequence FROM revs WHERE doc_id=? and revid < ?" +
        " and deleted=0 and json not null" +
        " ORDER BY sequence DESC LIMIT ?";
        String[] args = {Long.toString(docNumericID),generation+"-",Integer.toString(sqlLimit)};

        Cursor cursor = null;
        try {
            cursor = database.rawQuery(sql, args);
            cursor.moveToNext();
            while(!cursor.isAfterLast()){
                if (hasAttachment != null && revIDs.size() == 0){
                    hasAttachment.set(sequenceHasAttachments(cursor.getLong(1)));
                }
                revIDs.add(cursor.getString(0));
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting all revisions of document", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return revIDs;
    }


    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String findCommonAncestorOf(RevisionInternal rev, List<String> revIDs) {
        String result = null;

        if (revIDs.size() == 0)
            return null;
        String docId = rev.getDocId();
        long docNumericID = getDocNumericID(docId);
        if (docNumericID <= 0)
            return null;
        String quotedRevIds = joinQuoted(revIDs);
        String sql = "SELECT revid FROM revs " +
                "WHERE doc_id=? and revid in (" + quotedRevIds + ") and revid <= ? " +
                "ORDER BY revid DESC LIMIT 1";
        String[] args = {Long.toString(docNumericID)};

        Cursor cursor = null;
        try {
            cursor = database.rawQuery(sql, args);
            cursor.moveToNext();
            if (!cursor.isAfterLast()) {
                result = cursor.getString(0);
            }

        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting all revisions of document", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /**
     * Returns an array of TDRevs in reverse chronological order, starting with the given revision.
     * @exclude
     */
    @InterfaceAudience.Private
    public List<RevisionInternal> getRevisionHistory(RevisionInternal rev) {
        String docId = rev.getDocId();
        String revId = rev.getRevId();
        assert((docId != null) && (revId != null));

        long docNumericId = getDocNumericID(docId);
        if(docNumericId < 0) {
            return null;
        }
        else if(docNumericId == 0) {
            return new ArrayList<RevisionInternal>();
        }

        String sql = "SELECT sequence, parent, revid, deleted, json isnull FROM revs " +
                    "WHERE doc_id=? ORDER BY sequence DESC";
        String[] args = { Long.toString(docNumericId) };
        Cursor cursor = null;

        List<RevisionInternal> result;
        try {
            cursor = database.rawQuery(sql, args);

            cursor.moveToNext();
            long lastSequence = 0;
            result = new ArrayList<RevisionInternal>();
            while(!cursor.isAfterLast()) {
                long sequence = cursor.getLong(0);
                boolean matches = false;
                if(lastSequence == 0) {
                    matches = revId.equals(cursor.getString(2));
                }
                else {
                    matches = (sequence == lastSequence);
                }
                if(matches) {
                    revId = cursor.getString(2);
                    boolean deleted = (cursor.getInt(3) > 0);
                    boolean missing = (cursor.getInt(4) > 0);
                    RevisionInternal aRev = new RevisionInternal(docId, revId, deleted);
                    aRev.setMissing(missing);
                    aRev.setSequence(cursor.getLong(0));
                    result.add(aRev);
                    lastSequence = cursor.getLong(1);
                    if(lastSequence == 0) {
                        break;
                    }
                }
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting revision history", e);
            return null;
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /**
     * Splits a revision ID into its generation number and opaque suffix string
     * @exclude
     */
    @InterfaceAudience.Private
    public static int parseRevIDNumber(String rev) {
        int result = -1;
        int dashPos = rev.indexOf("-");
        if(dashPos >= 0) {
            try {
                result = Integer.parseInt(rev.substring(0, dashPos));
            } catch (NumberFormatException e) {
                // ignore, let it return -1
            }
        }
        return result;
    }

    /**
     * Splits a revision ID into its generation number and opaque suffix string
     * @exclude
     */
    @InterfaceAudience.Private
    public static String parseRevIDSuffix(String rev) {
        String result = null;
        int dashPos = rev.indexOf("-");
        if(dashPos >= 0) {
            result = rev.substring(dashPos + 1);
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static Map<String,Object> makeRevisionHistoryDict(List<RevisionInternal> history) {
        if(history == null) {
            return null;
        }

        // Try to extract descending numeric prefixes:
        List<String> suffixes = new ArrayList<String>();
        int start = -1;
        int lastRevNo = -1;
        for (RevisionInternal rev : history) {
            int revNo = parseRevIDNumber(rev.getRevId());
            String suffix = parseRevIDSuffix(rev.getRevId());
            if(revNo > 0 && suffix.length() > 0) {
                if(start < 0) {
                    start = revNo;
                }
                else if(revNo != lastRevNo - 1) {
                    start = -1;
                    break;
                }
                lastRevNo = revNo;
                suffixes.add(suffix);
            }
            else {
                start = -1;
                break;
            }
        }

        Map<String,Object> result = new HashMap<String,Object>();
        if(start == -1) {
            // we failed to build sequence, just stuff all the revs in list
            suffixes = new ArrayList<String>();
            for (RevisionInternal rev : history) {
                suffixes.add(rev.getRevId());
            }
        }
        else {
            result.put("start", start);
        }
        result.put("ids", suffixes);

        return result;
    }

    /**
     * Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option.
     * @exclude
     */
    @InterfaceAudience.Private
    public Map<String,Object> getRevisionHistoryDict(RevisionInternal rev) {
        return makeRevisionHistoryDict(getRevisionHistory(rev));
    }

    /**
     * Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option.
     * @exclude
     */
    @InterfaceAudience.Private
    public Map<String,Object> getRevisionHistoryDictStartingFromAnyAncestor(RevisionInternal rev, List<String>ancestorRevIDs) {
        List<RevisionInternal> history = getRevisionHistory(rev); // (this is in reverse order, newest..oldest
        if (ancestorRevIDs != null && ancestorRevIDs.size() > 0) {
            int n = history.size();
            for (int i = 0; i < n; ++i) {
                if (ancestorRevIDs.contains(history.get(i).getRevId())) {
                    history = history.subList(0, i+1);
                    break;
                }
            }
        }
        return makeRevisionHistoryDict(history);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionList changesSince(long lastSeq, ChangesOptions options, ReplicationFilter filter, Map<String, Object> filterParams) {
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
        String[] args = {Long.toString(lastSeq)};
        Cursor cursor = null;
        RevisionList changes = null;

        try {
            cursor = database.rawQuery(sql, args);
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
                if (includeDocs) {
                    expandStoredJSONIntoRevisionWithAttachments(cursor.getBlob(5), rev, options.getContentOptions());
                }
                if (runFilter(filter, filterParams, rev)) {
                    changes.add(rev);
                }
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error looking for changes", e);
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

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public boolean runFilter(ReplicationFilter filter, Map<String, Object> filterParams, RevisionInternal rev) {
        if (filter == null) {
            return true;
        }
        SavedRevision publicRev = new SavedRevision(this, rev);
        return filter.filter(publicRev, filterParams);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getDesignDocFunction(String fnName, String key, List<String>outLanguageList) {
        String[] path = fnName.split("/");
        if (path.length != 2) {
            return null;
        }
        String docId = String.format("_design/%s", path[0]);
        RevisionInternal rev = getDocumentWithIDAndRev(docId, null, EnumSet.noneOf(TDContentOptions.class));
        if (rev == null) {
            return null;
        }

        String outLanguage = (String) rev.getPropertyForKey("language");
        if (outLanguage != null) {
            outLanguageList.add(outLanguage);
        } else {
            outLanguageList.add("javascript");
        }
        Map<String, Object> container = (Map<String, Object>) rev.getPropertyForKey(key);
        return (String) container.get(path[1]);
    }


    /** VIEWS: **/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public View registerView(View view) {
        if(view == null) {
            return null;
        }
        if(views == null) {
            views = new HashMap<String,View>();
        }
        views.put(view.getName(), view);
        return view;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public List<QueryRow> queryViewNamed(String viewName, QueryOptions options, List<Long> outLastSequence) throws CouchbaseLiteException {

        long before = System.currentTimeMillis();
        long lastSequence = 0;
        List<QueryRow> rows = null;

        if (viewName != null && viewName.length() > 0) {
            final View view = getView(viewName);
            if (view == null) {
                throw new CouchbaseLiteException(new Status(Status.NOT_FOUND));
            }
            lastSequence = view.getLastSequenceIndexed();
            if (options.getStale() == Query.IndexUpdateMode.BEFORE || lastSequence <= 0) {
                view.updateIndex();
                lastSequence = view.getLastSequenceIndexed();
            } else if (options.getStale() == Query.IndexUpdateMode.AFTER && lastSequence < getLastSequenceNumber()) {

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            view.updateIndex();
                        } catch (CouchbaseLiteException e) {
                            Log.e(Database.TAG, "Error updating view index on background thread", e);
                        }
                    }
                }).start();

            }
            rows = view.queryWithOptions(options);


        } else {
            // nil view means query _all_docs
            // note: this is a little kludgy, but we have to pull out the "rows" field from the
            // result dictionary because that's what we want.  should be refactored, but
            // it's a little tricky, so postponing.
            Map<String,Object> allDocsResult = getAllDocs(options);
            rows = (List<QueryRow>) allDocsResult.get("rows");
            lastSequence = getLastSequenceNumber();
        }
        outLastSequence.add(lastSequence);

        long delta = System.currentTimeMillis() - before;
        Log.d(Database.TAG, "Query view %s completed in %d milliseconds", viewName, delta);

        return rows;

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    View makeAnonymousView() {
        for (int i=0; true; ++i) {
            String name = String.format("anon%d", i);
            View existing = getExistingView(name);
            if (existing == null) {
                // this name has not been used yet, so let's use it
                return getView(name);
            }
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public List<View> getAllViews() {
        Cursor cursor = null;
        List<View> result = null;

        try {
            cursor = database.rawQuery("SELECT name FROM views", null);
            cursor.moveToNext();
            result = new ArrayList<View>();
            while(!cursor.isAfterLast()) {
                result.add(getView(cursor.getString(0)));
                cursor.moveToNext();
            }
        } catch (Exception e) {
            Log.e(Database.TAG, "Error getting all views", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Status deleteViewNamed(String name) {
        Status result = new Status(Status.INTERNAL_SERVER_ERROR);
        try {
            if(views != null) {
                if(name != null) {
                    views.remove(name);
                }
            }
            String[] whereArgs = { name };
            int rowsAffected = database.delete("views", "name=?", whereArgs);
            if(rowsAffected > 0) {
                result.setCode(Status.OK);
            }
            else {
                result.setCode(Status.NOT_FOUND);
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error deleting view", e);
        }
        return result;
    }


    /**
     * Hack because cursor interface does not support cursor.getColumnIndex("deleted") yet.
     * @exclude
     */
    @InterfaceAudience.Private
    public int getDeletedColumnIndex(QueryOptions options) {
        if (options.isIncludeDocs()) {
            return 5;
        }
        else {
            return 4;
        }

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Map<String,Object> getAllDocs(QueryOptions options) throws CouchbaseLiteException {

        Map<String, Object> result = new HashMap<String, Object>();
        List<QueryRow> rows = new ArrayList<QueryRow>();
        if(options == null) {
            options = new QueryOptions();
        }
        boolean includeDeletedDocs = (options.getAllDocsMode() == Query.AllDocsMode.INCLUDE_DELETED);

        long updateSeq = 0;
        if(options.isUpdateSeq()) {
            updateSeq = getLastSequenceNumber();  // TODO: needs to be atomic with the following SELECT
        }

        StringBuffer sql = new StringBuffer("SELECT revs.doc_id, docid, revid, sequence");
        if (options.isIncludeDocs()) {
            sql.append(", json");
        }
        if (includeDeletedDocs) {
            sql.append(", deleted");
        }
        sql.append(" FROM revs, docs WHERE");
        if (options.getKeys() != null) {
            if (options.getKeys().size() == 0) {
                return result;
            }
            String commaSeperatedIds = joinQuotedObjects(options.getKeys());
            sql.append(String.format(" revs.doc_id IN (SELECT doc_id FROM docs WHERE docid IN (%s)) AND", commaSeperatedIds));
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
            assert(minKey instanceof String);
            sql.append((inclusiveMin ? " AND docid >= ?" :  " AND docid > ?"));
            args.add((String)minKey);
        }
        if (maxKey != null) {
            assert(maxKey instanceof String);
            maxKey = View.keyForPrefixMatch(maxKey, options.getPrefixMatchLevel());
            sql.append((inclusiveMax ? " AND docid <= ?" :  " AND docid < ?"));
            args.add((String)maxKey);
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

        Cursor cursor = null;
        Map<String, QueryRow> docs = new HashMap<String, QueryRow>();


        try {
            cursor = database.rawQuery(sql.toString(), args.toArray(new String[args.size()]));

            boolean keepGoing = cursor.moveToNext();

            while(keepGoing) {

                long docNumericID = cursor.getLong(0);
                String docId = cursor.getString(1);
                String revId = cursor.getString(2);
                long sequenceNumber = cursor.getLong(3);
                boolean deleted = includeDeletedDocs && cursor.getInt(getDeletedColumnIndex(options))>0;
                Map<String, Object> docContents = null;
                if (options.isIncludeDocs()) {
                    byte[] json = cursor.getBlob(4);
                    docContents = documentPropertiesFromJSON(json, docId, revId, deleted, sequenceNumber, options.getContentOptions());
                }

                // Iterate over following rows with the same doc_id -- these are conflicts.
                // Skip them, but collect their revIDs if the 'conflicts' option is set:
                List<String> conflicts = new ArrayList<String>();
                while (((keepGoing = cursor.moveToNext()) == true) && cursor.getLong(0) == docNumericID) {
                    if (options.getAllDocsMode() == Query.AllDocsMode.SHOW_CONFLICTS || options.getAllDocsMode() == Query.AllDocsMode.ONLY_CONFLICTS) {
                        if (conflicts.isEmpty()) {
                            conflicts.add(revId);
                        }
                        conflicts.add(cursor.getString(2));
                    }
                }

                if (options.getAllDocsMode() == Query.AllDocsMode.ONLY_CONFLICTS && conflicts.isEmpty()) {
                    continue;
                }

                Map<String, Object> value = new HashMap<String, Object>();
                value.put("rev", revId);
                value.put("_conflicts", conflicts);
                if (includeDeletedDocs){
                    value.put("deleted", (deleted ? true : null));
                }
                QueryRow change = new QueryRow(docId, sequenceNumber, docId, value, docContents);
                change.setDatabase(this);
                if (options.getKeys() != null) {
                    docs.put(docId, change);
                }
                // TODO: In the future, we need to implement CBLRowPassesFilter() in CBLView+Querying.m
                else if (options.getPostFilter() == null || options.getPostFilter().apply(change)) {
                    rows.add(change);
                }
            }

            if (options.getKeys() != null) {
                for (Object docIdObject : options.getKeys()) {
                    if (docIdObject instanceof String) {
                        String docId = (String) docIdObject;
                        QueryRow change = docs.get(docId);
                        if (change == null) {
                            Map<String, Object> value = new HashMap<String, Object>();
                            long docNumericID = getDocNumericID(docId);
                            if (docNumericID > 0) {
                                boolean deleted;
                                AtomicBoolean outIsDeleted = new AtomicBoolean(false);
                                AtomicBoolean outIsConflict = new AtomicBoolean();
                                String revId = winningRevIDOfDoc(docNumericID, outIsDeleted, outIsConflict);
                                if (outIsDeleted.get()) {
                                    deleted = true;
                                }
                                if (revId != null) {
                                    value.put("rev", revId);
                                    value.put("deleted", true);
                                }
                            }
                            change = new QueryRow((value != null ? docId : null), 0, docId, value, null);
                            change.setDatabase(this);
                        }
                        rows.add(change);
                    }
                }

            }


        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting all docs", e);
            throw new CouchbaseLiteException("Error getting all docs", e, new Status(Status.INTERNAL_SERVER_ERROR));
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        result.put("rows", rows);
        result.put("total_rows", rows.size());
        result.put("offset", options.getSkip());
        if(updateSeq != 0) {
            result.put("update_seq", updateSeq);
        }

        return result;
    }


    /**
     * Returns the rev ID of the 'winning' revision of this document, and whether it's deleted.
     * @exclude
     *
     * in CBLDatabase+Internal.m
     * - (NSString*) winningRevIDOfDocNumericID: (SInt64)docNumericID
     *                                isDeleted: (BOOL*)outIsDeleted
     *                               isConflict: (BOOL*)outIsConflict // optional
     *                                   status: (CBLStatus*)outStatus
     */
    @InterfaceAudience.Private
    String winningRevIDOfDoc(long docNumericId, AtomicBoolean outIsDeleted, AtomicBoolean outIsConflict) throws CouchbaseLiteException {

        Cursor cursor = null;
        String sql = "SELECT revid, deleted FROM revs" +
                " WHERE doc_id=? and current=1" +
                " ORDER BY deleted asc, revid desc LIMIT 2";

        String[] args = { Long.toString(docNumericId) };
        String revId = null;

        try {
            cursor = database.rawQuery(sql, args);

            if (cursor.moveToNext()) {
                revId = cursor.getString(0);
                outIsDeleted.set(cursor.getInt(1) > 0);
                // The document is in conflict if there are two+ result rows that are not deletions.
                if(outIsConflict != null) {
                    outIsConflict.set(!outIsDeleted.get() && cursor.moveToNext() && !(cursor.getInt(1) > 0));
                }
            } else {
                outIsDeleted.set(false);
                if(outIsConflict != null) {
                    outIsConflict.set(false);
                }
            }

        } catch (SQLException e) {
            Log.e(Database.TAG, "Error", e);
            throw new CouchbaseLiteException("Error", e, new Status(Status.INTERNAL_SERVER_ERROR));
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return revId;
    }


    /*************************************************************************************************/
    /*** Database+Attachments                                                                    ***/
    /*************************************************************************************************/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    void insertAttachmentForSequence(AttachmentInternal attachment, long sequence) throws CouchbaseLiteException {
        insertAttachmentForSequenceWithNameAndType(
                sequence,
                attachment.getName(),
                attachment.getContentType(),
                attachment.getRevpos(),
                attachment.getBlobKey(),
                attachment.getLength(),
                attachment.getEncoding(),
                attachment.getEncodedLength());
    }

    /**
     * Note: This method is used only from unit tests.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public void insertAttachmentForSequenceWithNameAndType(InputStream contentStream, long sequence, String name, String contentType, int revpos) throws CouchbaseLiteException {
        assert(sequence > 0);
        assert(name != null);

        BlobKey key = new BlobKey();
        if(!attachments.storeBlobStream(contentStream, key)) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }
        insertAttachmentForSequenceWithNameAndType(
                sequence,
                name,
                contentType,
                revpos,
                key);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void insertAttachmentForSequenceWithNameAndType(long sequence, String name, String contentType, int revpos, BlobKey key) throws CouchbaseLiteException {
        insertAttachmentForSequenceWithNameAndType(sequence, name, contentType, revpos, key, key != null ? attachments.getSizeOfBlob(key): -1, AttachmentInternal.AttachmentEncoding.AttachmentEncodingNone, -1);
    }

    private void insertAttachmentForSequenceWithNameAndType(long sequence, String name, String contentType, int revpos, BlobKey key, long length, AttachmentInternal.AttachmentEncoding encoding, long encodedLength) throws CouchbaseLiteException {
        try {
            ContentValues args = new ContentValues();
            args.put("sequence", sequence);
            args.put("filename", name);
            args.put("type", contentType);
            args.put("revpos", revpos);
            if (key != null) {
                args.put("key", key.getBytes());
            }
            if (length >= 0) {
                args.put("length", length);
            }
            if(encoding == AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP) {
                args.put("encoding", encoding.ordinal());
                if (encodedLength >= 0) {
                    args.put("encoded_length", encodedLength);
                }
            }
            long result = database.insert("attachments", null, args);
            if (result == -1) {
                String msg = "Insert attachment failed (returned -1)";
                Log.e(Database.TAG, msg);
                throw new CouchbaseLiteException(msg, Status.INTERNAL_SERVER_ERROR);
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error inserting attachment", e);
            throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    void installAttachment(AttachmentInternal attachment, Map<String, Object> attachInfo) throws CouchbaseLiteException {
        String digest = (String) attachInfo.get("digest");
        if (digest == null) {
            throw new CouchbaseLiteException(Status.BAD_ATTACHMENT);
        }

        if (pendingAttachmentsByDigest != null && pendingAttachmentsByDigest.containsKey(digest)) {
            BlobStoreWriter writer = pendingAttachmentsByDigest.get(digest);
            try {
                BlobStoreWriter blobStoreWriter = (BlobStoreWriter) writer;
                blobStoreWriter.install();
                attachment.setBlobKey(blobStoreWriter.getBlobKey());
                attachment.setLength(blobStoreWriter.getLength());
            } catch (Exception e) {
                throw new CouchbaseLiteException(e, Status.STATUS_ATTACHMENT_ERROR);
            }
        }
        else{
            Log.w(Database.TAG, "No pending attachment for digest: " + digest);
            throw new CouchbaseLiteException(Status.BAD_ATTACHMENT);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private Map<String, BlobStoreWriter> getPendingAttachmentsByDigest() {
        if (pendingAttachmentsByDigest == null) {
            pendingAttachmentsByDigest = new HashMap<String, BlobStoreWriter>();
        }
        return pendingAttachmentsByDigest;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void copyAttachmentNamedFromSequenceToSequence(String name, long fromSeq, long toSeq) throws CouchbaseLiteException {
        assert(name != null);
        assert(toSeq > 0);
        if(fromSeq < 0) {
            throw new CouchbaseLiteException(Status.NOT_FOUND);
        }

        Cursor cursor = null;

        String[] args = { Long.toString(toSeq), name, Long.toString(fromSeq), name };
        try {
            database.execSQL("INSERT INTO attachments (sequence, filename, key, type, length, revpos) " +
                    "SELECT ?, ?, key, type, length, revpos FROM attachments " +
                    "WHERE sequence=? AND filename=?", args);
            cursor = database.rawQuery("SELECT changes()", null);
            cursor.moveToNext();
            int rowsUpdated = cursor.getInt(0);
            if(rowsUpdated == 0) {
                // Oops. This means a glitch in our attachment-management or pull code,
                // or else a bug in the upstream server.
                Log.w(Database.TAG, "Can't find inherited attachment %s from seq# %s to copy to %s", name, fromSeq, toSeq);
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            }
            else {
                return;
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error copying attachment", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Returns the content and MIME type of an attachment
     * @exclude
     */
    @InterfaceAudience.Private
    public Attachment getAttachmentForSequence(long sequence, String filename) throws CouchbaseLiteException {
        assert(sequence > 0);
        assert(filename != null);

        Cursor cursor = null;

        String[] args = { Long.toString(sequence), filename };
        try {
            cursor = database.rawQuery("SELECT key, type FROM attachments WHERE sequence=? AND filename=?", args);

            if(!cursor.moveToNext()) {
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            }

            byte[] keyData = cursor.getBlob(0);
            //TODO add checks on key here? (ios version)
            BlobKey key = new BlobKey(keyData);
            InputStream contentStream = attachments.blobStreamForKey(key);
            if(contentStream == null) {
                Log.e(Database.TAG, "Failed to load attachment");
                throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
            }
            else {
                Attachment result = new Attachment(contentStream, cursor.getString(1));
                result.setGZipped(attachments.isGZipped(key));
                return result;
            }

        } catch (SQLException e) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

    }

    /**
     * Returns the location of an attachment's file in the blob store.
     * @exclude
     */
    @InterfaceAudience.Private
    String getAttachmentPathForSequence(long sequence, String filename) throws CouchbaseLiteException {

        assert(sequence > 0);
        assert(filename != null);
        Cursor cursor = null;
        String filePath = null;

        String args[] = { Long.toString(sequence), filename };
        try {
            cursor = database.rawQuery("SELECT key, type, encoding FROM attachments WHERE sequence=? AND filename=?", args);

            if(!cursor.moveToNext()) {
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            }

            byte[] keyData = cursor.getBlob(0);
            BlobKey key = new BlobKey(keyData);
            filePath = getAttachments().pathForKey(key);
            return filePath;

        } catch (SQLException e) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }


    public boolean sequenceHasAttachments(long sequence) {

        Cursor cursor = null;

        String args[] = { Long.toString(sequence) };
        try {
            cursor = database.rawQuery("SELECT 1 FROM attachments WHERE sequence=? LIMIT 1", args);

            if(cursor.moveToNext()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting attachments for sequence", e);
            return false;
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }


    /**
     * Constructs an "_attachments" dictionary for a revision, to be inserted in its JSON body.
     * @exclude
     */
    @InterfaceAudience.Private
    public Map<String,Object> getAttachmentsDictForSequenceWithContent(long sequence, EnumSet<TDContentOptions> contentOptions) {
        assert(sequence > 0);

        Cursor cursor = null;

        String args[] = { Long.toString(sequence) };
        try {
            cursor = database.rawQuery("SELECT filename, key, type, encoding, length, encoded_length, revpos FROM attachments WHERE sequence=?", args);

            if(!cursor.moveToNext()) {
                return null;
            }

            Map<String, Object> result = new HashMap<String, Object>();

            while(!cursor.isAfterLast()) {

                boolean dataSuppressed = false;
                int length = cursor.getInt(4);

                AttachmentInternal.AttachmentEncoding encoding = AttachmentInternal.AttachmentEncoding.values()[cursor.getInt(3)];
                int encodedLength = cursor.getInt(5);

                byte[] keyData = cursor.getBlob(1);
                BlobKey key = new BlobKey(keyData);
                String digestString = "sha1-" + Base64.encodeBytes(keyData);
                String dataBase64 = null;
                if(contentOptions.contains(TDContentOptions.TDIncludeAttachments)) {
                    if (contentOptions.contains(TDContentOptions.TDBigAttachmentsFollow) &&
                            length >= Database.kBigAttachmentLength) {
                        dataSuppressed = true;
                    }
                    else {
                        byte[] data = attachments.blobForKey(key);
                        if(data != null) {
                            dataBase64 = Base64.encodeBytes(data);  // <-- very expensive
                        }
                        else {
                            Log.w(Database.TAG, "Error loading attachment.  Sequence: %s", sequence);
                        }
                    }
                }

                String encodingStr = null;
                if (encoding == AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP) {
                    // NOTE: iOS decode if attachment is included int the dict.
                    encodingStr = "gzip";
                }

                Map<String, Object> attachment = new HashMap<String, Object>();
                if (!(dataBase64 != null || dataSuppressed)) {
                    attachment.put("stub", true);
                }
                if (dataBase64 != null) {
                    attachment.put("data", dataBase64);
                }
                if (dataSuppressed == true) {
                    attachment.put("follows", true);
                }
                attachment.put("digest", digestString);
                String contentType = cursor.getString(2);
                attachment.put("content_type", contentType);
                if (encodingStr != null) {
                    attachment.put("encoding", encodingStr);
                }
                attachment.put("length", length);
                if (encodingStr != null && encodedLength >= 0) {
                    attachment.put("encoded_length", encodedLength);
                }
                attachment.put("revpos", cursor.getInt(6));

                String filename = cursor.getString(0);
                result.put(filename, attachment);

                cursor.moveToNext();
            }

            return result;

        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting attachments for sequence", e);
            return null;
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    @InterfaceAudience.Private
    public URL fileForAttachmentDict(Map<String,Object> attachmentDict) {
        String digest = (String)attachmentDict.get("digest");
        if (digest == null) {
            return null;
        }
        String path = null;
        Object pending = pendingAttachmentsByDigest.get(digest);
        if (pending != null) {
            if (pending instanceof BlobStoreWriter) {
                path = ((BlobStoreWriter) pending).getFilePath();
            } else {
                BlobKey key = new BlobKey((byte[])pending);
                path = attachments.pathForKey(key);
            }
        } else {
            // If it's an installed attachment, ask the blob-store for it:
            BlobKey key = new BlobKey(digest);
            path = attachments.pathForKey(key);
        }

        URL retval = null;
        try {
            retval = new File(path).toURI().toURL();
        } catch (MalformedURLException e) {
            //NOOP: retval will be null
        }
        return retval;
    }


    /**
     * Modifies a RevisionInternal's body by changing all attachments with revpos < minRevPos into stubs.
     *
     * @exclude
     * @param rev
     * @param minRevPos
     */
    @InterfaceAudience.Private
    public void stubOutAttachmentsIn(RevisionInternal rev, int minRevPos)
    {
        if (minRevPos <= 1) {
            return;
        }
        Map<String, Object> properties = (Map<String,Object>)rev.getProperties();
        Map<String, Object> attachments = null;
        if(properties != null) {
            attachments = (Map<String,Object>)properties.get("_attachments");
        }
        Map<String, Object> editedProperties = null;
        Map<String, Object> editedAttachments = null;
        for (String name : attachments.keySet()) {
            Map<String,Object> attachment = (Map<String,Object>)attachments.get(name);
            int revPos = (Integer) attachment.get("revpos");
            Object stub = attachment.get("stub");
            if (revPos > 0 && revPos < minRevPos && (stub == null)) {
                // Strip this attachment's body. First make its dictionary mutable:
                if (editedProperties == null) {
                    editedProperties = new HashMap<String,Object>(properties);
                    editedAttachments = new HashMap<String,Object>(attachments);
                    editedProperties.put("_attachments", editedAttachments);
                }
                // ...then remove the 'data' and 'follows' key:
                Map<String,Object> editedAttachment = new HashMap<String,Object>(attachment);
                editedAttachment.remove("data");
                editedAttachment.remove("follows");
                editedAttachment.put("stub", true);
                editedAttachments.put(name,editedAttachment);
                Log.v(Database.TAG, "Stubbed out attachment.  minRevPos: %s rev: %s name: %s revpos: %s", minRevPos, rev, name, revPos);
            }
        }
        if (editedProperties != null)
            rev.setProperties(editedProperties);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    void stubOutAttachmentsInRevision(final Map<String, AttachmentInternal> attachments, final RevisionInternal rev) {

        rev.mutateAttachments(new CollectionUtils.Functor<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> invoke(Map<String, Object> attachment) {
                if (attachment.containsKey("follows") || attachment.containsKey("data")) {
                    Map<String, Object> editedAttachment = new HashMap<String, Object>(attachment);
                    editedAttachment.remove("follows");
                    editedAttachment.remove("data");
                    editedAttachment.put("stub",true);
                    if(!editedAttachment.containsKey("revpos")) {
                        editedAttachment.put("revpos",rev.getGeneration());
                    }

                    AttachmentInternal attachmentObject = attachments.get(name);
                    if(attachmentObject != null) {
                        editedAttachment.put("length",attachmentObject.getLength());
                        editedAttachment.put("digest", attachmentObject.getBlobKey().base64Digest());
                    }
                    attachment = editedAttachment;
                }
                return attachment;
            }
        });

    }

    // Replaces attachment data whose revpos is < minRevPos with stubs.
    // If attachmentsFollow==YES, replaces data with "follows" key.
    public static void stubOutAttachmentsInRevBeforeRevPos(final RevisionInternal rev, final int minRevPos, final boolean attachmentsFollow) {
        if (minRevPos <= 1 && !attachmentsFollow) {
            return;
        }

        rev.mutateAttachments(new CollectionUtils.Functor<Map<String,Object>,Map<String,Object>>() {
            public Map<String, Object> invoke(Map<String, Object> attachment) {
                int revPos = 0;
                if (attachment.get("revpos") != null) {
                    revPos = (Integer)attachment.get("revpos");
                }

                boolean includeAttachment = (revPos == 0 || revPos >= minRevPos);
                boolean stubItOut = !includeAttachment && (attachment.get("stub") == null || (Boolean)attachment.get("stub") == false);
                boolean addFollows = includeAttachment && attachmentsFollow && (attachment.get("follows") == null || (Boolean)attachment.get("follows") == false);

                if (!stubItOut && !addFollows) {
                    return attachment;  // no change
                }

                // Need to modify attachment entry:
                Map<String, Object> editedAttachment = new HashMap<String, Object>(attachment);
                editedAttachment.remove("data");
                if (stubItOut) {
                    // ...then remove the 'data' and 'follows' key:
                    editedAttachment.remove("follows");
                    editedAttachment.put("stub",true);
                    Log.v(Log.TAG_SYNC, "Stubbed out attachment %s: revpos %d < %d", rev, revPos, minRevPos);
                } else if (addFollows) {
                    editedAttachment.remove("stub");
                    editedAttachment.put("follows",true);
                    Log.v(Log.TAG_SYNC, "Added 'follows' for attachment %s: revpos %d >= %d",rev, revPos, minRevPos);
                }
                return editedAttachment;
            }
        });
    }

    // Replaces the "follows" key with the real attachment data in all attachments to 'doc'.
    public boolean inlineFollowingAttachmentsIn(RevisionInternal rev) {

        return rev.mutateAttachments(new CollectionUtils.Functor<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> invoke(Map<String, Object> attachment) {
                if (!attachment.containsKey("follows")) {
                    return attachment;
                }
                URL fileURL = fileForAttachmentDict(attachment);
                byte[] fileData = null;
                try {
                    InputStream is = fileURL.openStream();
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    StreamUtils.copyStream(is,os);
                    fileData = os.toByteArray();
                } catch (IOException e) {
                    Log.e(Log.TAG_SYNC,"could not retrieve attachment data: %S",e);
                    return null;
                }

                Map<String, Object> editedAttachment = new HashMap<String, Object>(attachment);
                editedAttachment.remove("follows");
                editedAttachment.put("data",Base64.encodeBytes(fileData));
                return editedAttachment;
            }
        });
    }


    /**
     * Given a newly-added revision, adds the necessary attachment rows to the sqliteDb and
     * stores inline attachments into the blob store.
     * @exclude
     */
    @InterfaceAudience.Private
    void processAttachmentsForRevision(Map<String, AttachmentInternal> attachments, RevisionInternal rev, long parentSequence, RevisionList localRevsList) throws CouchbaseLiteException {

        assert(rev != null);
        long newSequence = rev.getSequence();
        assert(newSequence > parentSequence);
        int generation = rev.getGeneration();
        assert(generation > 0);

        // If there are no attachments in the new rev, there's nothing to do:
        Map<String,Object> revAttachments = null;
        Map<String,Object> properties = (Map<String,Object>)rev.getProperties();
        if(properties != null) {
            revAttachments = (Map<String,Object>)properties.get("_attachments");
        }
        if(revAttachments == null || revAttachments.size() == 0 || rev.isDeleted()) {
            return;
        }

        for (String name : revAttachments.keySet()) {
            AttachmentInternal attachment = attachments.get(name);
            if (attachment != null) {
                // Determine the revpos, i.e. generation # this was added in. Usually this is
                // implicit, but a rev being pulled in replication will have it set already.
                if (attachment.getRevpos() == 0) {
                    attachment.setRevpos(generation);
                }
                else if (attachment.getRevpos() > generation) {
                    Log.w(Database.TAG, "Attachment %s %s has unexpected revpos %s, setting to %s", rev, name, attachment.getRevpos(), generation);
                    attachment.setRevpos(generation);
                }
                // Finally insert the attachment:
                insertAttachmentForSequence(attachment, newSequence);

            }
            else {

                long attachmentSequence = parentSequence;
                // try to find the right sequence for the attachment
                if(localRevsList != null) {
                    int revpos = (Integer) ((Map) revAttachments.get(name)).get("revpos");
                    RevisionInternal attachmentRev = localRevsList.revWithDocIdAndGeneration(rev.getDocId(), revpos);
                    if (attachmentRev != null) {
                        attachmentSequence = attachmentRev.getSequence();
                    }
                }
                // It's just a stub, so copy the previous revision's attachment entry:
                //? Should I enforce that the type and digest (if any) match?
                copyAttachmentNamedFromSequenceToSequence(name, attachmentSequence, newSequence);
            }

        }

    }


    /**
     * Updates or deletes an attachment, creating a new document revision in the process.
     * Used by the PUT / DELETE methods called on attachment URLs.
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionInternal updateAttachment(String filename, BlobStoreWriter body, String contentType, AttachmentInternal.AttachmentEncoding encoding, String docID, String oldRevID) throws CouchbaseLiteException {

        boolean isSuccessful = false;

        if(filename == null || filename.length() == 0 || (body != null && contentType == null) || (oldRevID != null && docID == null) || (body != null && docID == null)) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        beginTransaction();
        try {
            RevisionInternal oldRev = new RevisionInternal(docID, oldRevID, false);
            if(oldRevID != null) {

                // Load existing revision if this is a replacement:
                try {
                    loadRevisionBody(oldRev, EnumSet.noneOf(TDContentOptions.class));
                } catch (CouchbaseLiteException e) {
                    if (e.getCBLStatus().getCode() == Status.NOT_FOUND && existsDocumentWithIDAndRev(docID, null) ) {
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    }
                }

            } else {
                // If this creates a new doc, it needs a body:
                oldRev.setBody(new Body(new HashMap<String,Object>()));
            }

            // Update the _attachments dictionary:
            Map<String, Object> oldRevProps = oldRev.getProperties();
            Map<String,Object> attachments = null;
            if (oldRevProps != null) {
                attachments = (Map<String, Object>) oldRevProps.get("_attachments");
            }

            if (attachments == null)
                attachments = new HashMap<String, Object>();

            if (body != null) {
                BlobKey key = body.getBlobKey();
                String digest = key.base64Digest();

                Map<String, BlobStoreWriter> blobsByDigest = new HashMap<String, BlobStoreWriter>();
                blobsByDigest.put(digest,body);
                rememberAttachmentWritersForDigests(blobsByDigest);

                String encodingName = (encoding == AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP) ? "gzip" : null;
                Map<String,Object> dict = new HashMap<String, Object>();

                dict.put("digest", digest);
                dict.put("length", body.getLength());
                dict.put("follows", true);
                dict.put("content_type", contentType);
                dict.put("encoding", encodingName);

                attachments.put(filename, dict);
            } else {
                if (oldRevID != null && !attachments.containsKey(filename) ) {
                    throw new CouchbaseLiteException(Status.NOT_FOUND);
                }
                attachments.remove(filename);
            }

            Map<String, Object> properties = oldRev.getProperties();
            properties.put("_attachments",attachments);
            oldRev.setProperties(properties);


            // Create a new revision:
            Status putStatus = new Status();
            RevisionInternal newRev = putRevision(oldRev, oldRevID, false, putStatus);

            isSuccessful = true;
            return newRev;

        } catch(SQLException e) {
            Log.e(TAG, "Error updating attachment", e);
            throw new CouchbaseLiteException(new Status(Status.INTERNAL_SERVER_ERROR));
        } finally {
            endTransaction(isSuccessful);
        }

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void rememberAttachmentWritersForDigests(Map<String, BlobStoreWriter> blobsByDigest) {

        getPendingAttachmentsByDigest().putAll(blobsByDigest);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    void rememberAttachmentWriter(BlobStoreWriter writer) {
        getPendingAttachmentsByDigest().put(writer.mD5DigestString(), writer);
    }


     /**
      * Deletes obsolete attachments from the sqliteDb and blob store.
      * @exclude
      */
     @InterfaceAudience.Private
    public Status garbageCollectAttachments() {
        // First delete attachment rows for already-cleared revisions:
        // OPT: Could start after last sequence# we GC'd up to

        try {
            database.execSQL("DELETE FROM attachments WHERE sequence IN " +
                    "(SELECT sequence from revs WHERE json IS null)");
        }
        catch(SQLException e) {
            Log.e(Database.TAG, "Error deleting attachments", e);
        }

        // Now collect all remaining attachment IDs and tell the store to delete all but these:
        Cursor cursor = null;
        try {
            cursor = database.rawQuery("SELECT DISTINCT key FROM attachments", null);

            cursor.moveToNext();
            List<BlobKey> allKeys = new ArrayList<BlobKey>();
            while(!cursor.isAfterLast()) {
                BlobKey key = new BlobKey(cursor.getBlob(0));
                allKeys.add(key);
                cursor.moveToNext();
            }

            int numDeleted = attachments.deleteBlobsExceptWithKeys(allKeys);
            if(numDeleted < 0) {
                return new Status(Status.INTERNAL_SERVER_ERROR);
            }

            Log.v(Database.TAG, "Deleted %d attachments", numDeleted);

            return new Status(Status.OK);
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error finding attachment keys in use", e);
            return new Status(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
    }

    /*************************************************************************************************/
    /*** Database+Insertion                                                                      ***/
    /*************************************************************************************************/

    /** DOCUMENT & REV IDS: **/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static boolean isValidDocumentId(String id) {
        // http://wiki.apache.org/couchdb/HTTP_Document_API#Documents
        if(id == null || id.length() == 0) {
            return false;
        }
        if(id.charAt(0) == '_') {
            return  (id.startsWith("_design/"));
        }
        return true;
        // "_local/*" is not a valid document ID. Local docs have their own API and shouldn't get here.
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static String generateDocumentId() {
        return Misc.TDCreateUUID();
    }

    /**
     * in CBLDatabase+Insertion.m
     * - (NSString*) generateIDForRevision: (CBL_Revision*)rev
     *                            withJSON: (NSData*)json
     *                         attachments: (NSDictionary*)attachments
     *                              prevID: (NSString*) prevID
     * @exclude
     */
    @InterfaceAudience.Private
    public String generateIDForRevision(RevisionInternal rev, byte[] json, Map<String, AttachmentInternal> attachments, String previousRevisionId) {

        MessageDigest md5Digest;

        // Revision IDs have a generation count, a hyphen, and a UUID.

        int generation = 0;
        if(previousRevisionId != null) {
            generation = RevisionInternal.generationFromRevID(previousRevisionId);
            if(generation == 0) {
                return null;
            }
        }

        // Generate a digest for this revision based on the previous revision ID, document JSON,
        // and attachment digests. This doesn't need to be secure; we just need to ensure that this
        // code consistently generates the same ID given equivalent revisions.
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        // single byte - length of previous revision id
        // +
        // previous revision id
        int length = 0;
        byte[] prevIDUTF8 = null;
        if (previousRevisionId != null) {
            prevIDUTF8 = previousRevisionId.getBytes(Charset.forName("UTF-8"));
            length = prevIDUTF8.length;
        }
        if (length > 0xFF) {
            return null;
        }
        byte lengthByte = (byte) (length & 0xFF);
        byte[] lengthBytes = new byte[] { lengthByte };
        md5Digest.update(lengthBytes); // prefix with length byte
        if (length > 0 && prevIDUTF8 != null) {
            md5Digest.update(prevIDUTF8);
        }

        // single byte - deletion flag
        int isDeleted = ((rev.isDeleted() != false) ? 1 : 0);
        byte[] deletedByte = new byte[] { (byte) isDeleted };
        md5Digest.update(deletedByte);

        // all attachment keys
        List<String> attachmentKeys = new ArrayList<String>(attachments.keySet());
        Collections.sort(attachmentKeys);
        for (String key : attachmentKeys) {
            AttachmentInternal attachment = attachments.get(key);
            md5Digest.update(attachment.getBlobKey().getBytes());
        }

        // json
        if (json != null) {
            md5Digest.update(json);
        }
        byte[] md5DigestResult = md5Digest.digest();

        String digestAsHex = Utils.bytesToHex(md5DigestResult);

        int generationIncremented = generation + 1;
        return String.format("%d-%s", generationIncremented, digestAsHex).toLowerCase();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public long insertDocumentID(String docId) {
        long rowId = -1;
        try {
            ContentValues args = new ContentValues();
            args.put("docid", docId);
            rowId = database.insert("docs", null, args);
        } catch (Exception e) {
            Log.e(Database.TAG, "Error inserting document id", e);
        }
        return rowId;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public long getOrInsertDocNumericID(String docId) {
        long docNumericId = getDocNumericID(docId);
        if(docNumericId == 0) {
            docNumericId = insertDocumentID(docId);
        }
        return docNumericId;
    }

    /**
     * Parses the _revisions dict from a document into an array of revision ID strings
     * @exclude
     */
    @InterfaceAudience.Private
    public static List<String> parseCouchDBRevisionHistory(Map<String,Object> docProperties) {
        Map<String,Object> revisions = (Map<String,Object>)docProperties.get("_revisions");
        if(revisions == null) {
            return new ArrayList<String>();
        }
        List<String> revIDs = new ArrayList<String>((List<String>)revisions.get("ids"));
        if (revIDs == null || revIDs.isEmpty()) {
            return new ArrayList<String>();
        }
        Integer start = (Integer)revisions.get("start");
        if(start != null) {
            for(int i=0; i < revIDs.size(); i++) {
                String revID = revIDs.get(i);
                revIDs.set(i, Integer.toString(start--) + "-" + revID);
            }
        }
        return revIDs;
    }

    /** INSERTION: **/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public byte[] encodeDocumentJSON(RevisionInternal rev) {

        Map<String,Object> origProps = rev.getProperties();
        if(origProps == null) {
            return null;
        }

        List<String> specialKeysToLeave = Arrays.asList(
                "_removed",
                "_replication_id",
                "_replication_state",
                "_replication_state_time");

        // Don't allow any "_"-prefixed keys. Known ones we'll ignore, unknown ones are an error.
        Map<String,Object> properties = new HashMap<String,Object>(origProps.size());
        for (String key : origProps.keySet()) {
            boolean shouldAdd = false;
            if(key.startsWith("_")) {
                if(!KNOWN_SPECIAL_KEYS.contains(key)) {
                    Log.e(TAG, "Database: Invalid top-level key '%s' in document to be inserted", key);
                    return null;
                }
                if (specialKeysToLeave.contains(key)) {
                    shouldAdd = true;
                }
            } else {
                shouldAdd = true;
            }
            if (shouldAdd) {
                properties.put(key, origProps.get(key));
            }
        }

        byte[] json = null;
        try {
            json = Manager.getObjectMapper().writeValueAsBytes(properties);
        } catch (Exception e) {
            Log.e(Database.TAG, "Error serializing " + rev + " to JSON", e);
        }
        return json;
    }


    @InterfaceAudience.Private
    private boolean postChangeNotifications() {
        boolean posted = false;
        int tLevel = transactionLevel.get();
        // This is a 'while' instead of an 'if' because when we finish posting notifications, there
        // might be new ones that have arrived as a result of notification handlers making document
        // changes of their own (the replicator manager will do this.) So we need to check again.
        while (tLevel == 0 && isOpen() && !postingChangeNotifications
                && changesToNotify.size() > 0)
        {

            try {
                postingChangeNotifications = true; // Disallow re-entrant calls

                List<DocumentChange> outgoingChanges = new ArrayList<DocumentChange>();
                outgoingChanges.addAll(changesToNotify);
                changesToNotify.clear();

                // TODO: postPublicChangeNotification in CBLDatabase+Internal.m should replace following lines of code.

                boolean isExternal = false;
                for (DocumentChange change : outgoingChanges) {
                    Document doc = cachedDocumentWithID(change.getDocumentId());
                    if (doc != null) {
                        doc.revisionAdded(change, true);
                    }
                    if (change.getSourceUrl() != null) {
                        isExternal = true;
                    }
                }

                ChangeEvent changeEvent = new ChangeEvent(this, isExternal, outgoingChanges);

                for (ChangeListener changeListener : changeListeners) {
                    changeListener.changed(changeEvent);
                }

                posted = true;
            } catch (Exception e) {
                Log.e(Database.TAG, this + " got exception posting change notifications", e);
            } finally {
                postingChangeNotifications = false;
            }
        }
        return posted;
    }

    /**
     * in CBLDatabase+Internal.m
     * - (void) databaseStorageChanged:(CBLDatabaseChange *)change
     */
    private void databaseStorageChanged(DocumentChange change) {
        Log.v(Log.TAG_DATABASE, "Added: " + change.getAddedRevision());
        if (changesToNotify == null) {
            changesToNotify = new ArrayList<DocumentChange>();
        }
        changesToNotify.add(change);
        if (!postChangeNotifications()) {
            // The notification wasn't posted yet, probably because a transaction is open.
            // But the CBLDocument, if any, needs to know right away so it can update its
            // currentRevision.

            Document doc = cachedDocumentWithID(change.getDocumentId());
            if (doc != null) {
                doc.revisionAdded(change, false);
            }
        }

        // Squish the change objects if too many of them are piling up:
        if (changesToNotify.size() >= MANY_CHANGES_TO_NOTIFY) {
            if(changesToNotify.size() == MANY_CHANGES_TO_NOTIFY){
                for(DocumentChange c : changesToNotify){
                    c.reduceMemoryUsage();
                }
            }else{
                change.reduceMemoryUsage();
            }
        }
    }

    /**
     * in CBLDatabase+Internal.m
     * - (void) storageExitedTransaction: (BOOL)committed
     */
    private void storageExitedTransaction(boolean committed) {
        if (!committed) {
            // I already told cached CBLDocuments about these new revisions. Back that out:
            for (DocumentChange change : changesToNotify) {
                Document doc = cachedDocumentWithID(change.getDocumentId());
                if (doc != null) {
                    doc.forgetCurrentRevision();
                }
            }
            changesToNotify.clear();
        }
        postChangeNotifications();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public long insertRevision(RevisionInternal rev, long docNumericID, long parentSequence, boolean current, boolean hasAttachments, byte[] data) {
        long rowId = 0;
        try {
            ContentValues args = new ContentValues();
            args.put("doc_id", docNumericID);
            args.put("revid", rev.getRevId());
            if(parentSequence != 0) {
                args.put("parent", parentSequence);
            }
            args.put("current", current);
            args.put("deleted", rev.isDeleted());
            args.put("no_attachments",!hasAttachments);
            args.put("json", data);
            rowId = database.insert("revs", null, args);
            rev.setSequence(rowId);
        } catch (Exception e) {
            Log.e(Database.TAG, "Error inserting revision", e);
        }
        return rowId;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionInternal putRevision(RevisionInternal rev, String prevRevId, Status resultStatus) throws CouchbaseLiteException {
        return putRevision(rev, prevRevId, false, resultStatus);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionInternal putRevision(RevisionInternal rev, String prevRevId,  boolean allowConflict) throws CouchbaseLiteException {
        Status ignoredStatus = new Status();
        return putRevision(rev, prevRevId, allowConflict, ignoredStatus);
    }


    /**
     * Stores a new (or initial) revision of a document.
     *
     * This is what's invoked by a PUT or POST. As with those, the previous revision ID must be supplied when necessary and the call will fail if it doesn't match.
     *
     * @param oldRev The revision to add. If the docID is null, a new UUID will be assigned. Its revID must be null. It must have a JSON body.
     * @param prevRevId The ID of the revision to replace (same as the "?rev=" parameter to a PUT), or null if this is a new document.
     * @param allowConflict If false, an error status 409 will be returned if the insertion would create a conflict, i.e. if the previous revision already has a child.
     * @param resultStatus On return, an HTTP status code indicating success or failure.
     * @return A new RevisionInternal with the docID, revID and sequence filled in (but no body).
     * @exclude
     */
    @SuppressWarnings("unchecked")
    @InterfaceAudience.Private
    public RevisionInternal putRevision(RevisionInternal oldRev, String prevRevId, boolean allowConflict, Status resultStatus) throws CouchbaseLiteException {
        // prevRevId is the rev ID being replaced, or nil if an insert
        String docId = oldRev.getDocId();
        boolean deleted = oldRev.isDeleted();
        if((oldRev == null) || ((prevRevId != null) && (docId == null)) || (deleted && (docId == null))
                || ((docId != null) && !isValidDocumentId(docId))) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        beginTransaction();
        Cursor cursor = null;
        boolean inConflict = false;
        RevisionInternal winningRev = null;
        RevisionInternal newRev = null;

        //// PART I: In which are performed lookups and validations prior to the insert...

        long docNumericID = (docId != null) ? getDocNumericID(docId) : 0;
        long parentSequence = 0;
        AtomicBoolean oldWinnerWasDeletion = new AtomicBoolean(false);
        AtomicBoolean wasConflicted = new AtomicBoolean(false);
        String oldWinningRevID = null;

        try {
            if (docNumericID > 0) {
                try {
                    oldWinningRevID = winningRevIDOfDoc(docNumericID, oldWinnerWasDeletion, wasConflicted);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            if(prevRevId != null) {
                // Replacing: make sure given prevRevID is current & find its sequence number:
                if(docNumericID <= 0) {
                    String msg = String.format("No existing revision found with doc id: %s", docId);
                    throw new CouchbaseLiteException(msg ,Status.NOT_FOUND);
                }

                parentSequence = getSequenceOfDocument(docNumericID, prevRevId, !allowConflict);

                if(parentSequence == 0) {
                    // Not found: either a 404 or a 409, depending on whether there is any current revision
                    if(!allowConflict && existsDocumentWithIDAndRev(docId, null)) {
                        String msg = String.format("Conflicts not allowed and there is already an existing doc with id: %s", docId);
                        throw new CouchbaseLiteException(msg, Status.CONFLICT);
                    }
                    else {
                        String msg = String.format("No existing revision found with doc id: %s", docId);
                        throw new CouchbaseLiteException(msg, Status.NOT_FOUND);
                    }
                }

                if(validations != null && validations.size() > 0) {
                    // Fetch the previous revision and validate the new one against it:
                    RevisionInternal fakeNewRev = oldRev.copyWithDocID(oldRev.getDocId(), null);
                    RevisionInternal prevRev = new RevisionInternal(docId, prevRevId, false);
                    validateRevision(fakeNewRev, prevRev,prevRevId);
                }

            }
            else {
                // Inserting first revision.
                if(deleted && (docId != null)) {
                    // Didn't specify a revision to delete: 404 or a 409, depending
                    if(existsDocumentWithIDAndRev(docId, null)) {
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    }
                    else {
                        throw new CouchbaseLiteException(Status.NOT_FOUND);
                    }
                }

                // Validate:
                validateRevision(oldRev, null, null);

                if(docId != null) {
                    // Inserting first revision, with docID given (PUT):
                    if(docNumericID <= 0) {
                        // Doc doesn't exist at all; create it:
                        docNumericID = insertDocumentID(docId);
                        if(docNumericID <= 0) {
                            return null;
                        }
                    } else {

                        // Doc ID exists; check whether current winning revision is deleted:
                        if (oldWinnerWasDeletion.get() == true) {
                            prevRevId = oldWinningRevID;
                            parentSequence = getSequenceOfDocument(docNumericID, prevRevId, false);

                        } else if (oldWinningRevID != null) {
                            String msg = "The current winning revision is not deleted, so this is a conflict";
                            throw new CouchbaseLiteException(msg, Status.CONFLICT);
                        }

                    }
                }
                else {
                    // Inserting first revision, with no docID given (POST): generate a unique docID:
                    docId = Database.generateDocumentId();
                    docNumericID = insertDocumentID(docId);
                    if(docNumericID <= 0) {
                        return null;
                    }
                }
            }

            // There may be a conflict if (a) the document was already in conflict, or
            // (b) a conflict is created by adding a non-deletion child of a non-winning rev.
            inConflict = wasConflicted.get() ||
                    (!deleted &&
                            prevRevId != null &&
                            oldWinningRevID != null &&
                            !prevRevId.equals(oldWinningRevID));


            //// PART II: In which we prepare for insertion...

            // Get the attachments:
            Map<String, AttachmentInternal> attachments = getAttachmentsFromRevision(oldRev, prevRevId);

            // Bump the revID and update the JSON:
            byte[] json = null;
            if(oldRev.getProperties() != null && oldRev.getProperties().size() > 0) {
                json = encodeDocumentJSON(oldRev);
                if(json == null) {
                    String msg = "Bad or missing JSON";
                    throw new CouchbaseLiteException(msg, Status.BAD_REQUEST);
                }

                if(json.length == 2 && json[0] == '{' && json[1] == '}') {
                    json = null;
                }

            }

            String newRevId = generateIDForRevision(oldRev, json, attachments, prevRevId);
            newRev = oldRev.copyWithDocID(docId, newRevId);
            stubOutAttachmentsInRevision(attachments, newRev);

            // Don't store a SQL null in the 'json' column -- I reserve it to mean that the revision data
            // is missing due to compaction or replication.
            // Instead, store an empty zero-length blob.
            if(json == null)
                json = new byte[0];

            //// PART III: In which the actual insertion finally takes place:
            // Now insert the rev itself:
            long newSequence = insertRevision(newRev, docNumericID, parentSequence, true, (attachments != null), json);
            if(newSequence <= 0) {
                // The insert failed. If it was due to a constraint violation, that means a revision
                // already exists with identical contents and the same parent rev. We can ignore this
                // insert call, then.
                // TODO - figure out database error code
                // NOTE - In AndroidSQLiteStorageEngine.java, CBL Android uses insert() method of SQLiteDatabase
                //        which return -1 for error case. Instead of insert(), should use insertOrThrow method()
                //        which throws detailed error code. Need to check with SQLiteDatabase.CONFLICT_FAIL.
                //        However, returning after updating parentSequence might not cause any problem.
                //if (_fmdb.lastErrorCode != SQLITE_CONSTRAINT)
                //    return null;
                Log.w(Database.TAG, "Duplicate rev insertion: " + docId + " / " + newRevId);
                newRev.setBody(null);
                // don't return yet; update the parent's current just to be sure (see #316 (iOS #509))
            }

            // Make replaced rev non-current:
            try {
                ContentValues args = new ContentValues();
                args.put("current", 0);
                database.update("revs", args, "sequence=?", new String[] {String.valueOf(parentSequence)});
            } catch (SQLException e) {
                Log.e(Database.TAG, "Error setting parent rev non-current", e);
                throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
            }

            if(newSequence <= 0) {
                // duplicate rev; see above
                resultStatus.setCode(Status.OK);
                databaseStorageChanged(new DocumentChange(newRev, winningRev, inConflict, null));
                return newRev;
            }

            // Store any attachments:
            if(attachments != null) {
                processAttachmentsForRevision(attachments, newRev, parentSequence, null);
            }


            // Figure out what the new winning rev ID is:
            winningRev = winner(docNumericID, oldWinningRevID, oldWinnerWasDeletion.get(), newRev);

            // Success!
            if(deleted) {
                resultStatus.setCode(Status.OK);
            }
            else {
                resultStatus.setCode(Status.CREATED);
            }

        } catch (SQLException e1) {
            Log.e(Database.TAG, "Error putting revision", e1);
            return null;
        } finally {
            if(cursor != null) {
                cursor.close();
            }
            endTransaction(resultStatus.isSuccessful());
        }

        //// EPILOGUE: A change notification is sent...
        databaseStorageChanged(new DocumentChange(newRev, winningRev, inConflict, null));
        return newRev;
    }

    /**
     * @exclude
     * in CBLDatabase+Insertion.m
     * - (CBL_Revision*) winnerWithDocID: (SInt64)docNumericID
     *                         oldWinner: (NSString*)oldWinningRevID
     *                        oldDeleted: (BOOL)oldWinnerWasDeletion
     *                            newRev: (CBL_Revision*)newRev
     */
    @InterfaceAudience.Private
    RevisionInternal winner(long docNumericID,
                            String oldWinningRevID,
                            boolean oldWinnerWasDeletion,
                            RevisionInternal newRev) throws CouchbaseLiteException {


        if (oldWinningRevID == null) {
            return newRev;
        }
        String newRevID = newRev.getRevId();
        if (!newRev.isDeleted()) {
            if (oldWinnerWasDeletion ||
                    RevisionInternal.CBLCompareRevIDs(newRevID, oldWinningRevID) > 0) {
                return newRev; // this is now the winning live revision
            }
        } else if (oldWinnerWasDeletion) {
            if (RevisionInternal.CBLCompareRevIDs(newRevID, oldWinningRevID) > 0) {
                return newRev;  // doc still deleted, but this beats previous deletion rev
            }
        } else {
            // Doc was alive. How does this deletion affect the winning rev ID?
            AtomicBoolean outIsDeleted = new AtomicBoolean(false);
            AtomicBoolean outIsConflict = new AtomicBoolean(false);
            String winningRevID = winningRevIDOfDoc(docNumericID, outIsDeleted, outIsConflict);
            if (!winningRevID.equals(oldWinningRevID)) {
                if (winningRevID.equals(newRev.getRevId())) {
                    return newRev;
                } else {
                    boolean deleted = false;
                    RevisionInternal winningRev = new RevisionInternal(newRev.getDocId(), winningRevID, deleted);
                    return winningRev;
                }
            }
        }
        return null; // no change

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private long getSequenceOfDocument(long docNumericId, String revId, boolean onlyCurrent) {

        long result = -1;
        Cursor cursor = null;
        try {
            String extraSql = (onlyCurrent ? "AND current=1" : "");
            String sql = String.format("SELECT sequence FROM revs WHERE doc_id=? AND revid=? %s LIMIT 1", extraSql);
            String[] args = { ""+docNumericId, revId };
            cursor = database.rawQuery(sql, args);

            if(cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
            else {
                result = 0;
            }
        } catch (Exception e) {
            Log.e(Database.TAG, "Error getting getSequenceOfDocument", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    Map<String, Object> getAttachments(String docID, String revID) {
        RevisionInternal mrev = new RevisionInternal(docID, revID, false);
        try {
            RevisionInternal rev = loadRevisionBody(mrev, EnumSet.noneOf(TDContentOptions.class));
            return rev.getAttachments();
        } catch (CouchbaseLiteException e) {
            Log.w(Log.TAG_DATABASE, "Failed to get attachments for " + mrev, e);
            return null;
        }
    }

    /**
     * Given a revision, read its _attachments dictionary (if any), convert each attachment to a
     * AttachmentInternal object, and return a dictionary mapping names->CBL_Attachments.
     * @exclude
     */
    @InterfaceAudience.Private
    Map<String, AttachmentInternal> getAttachmentsFromRevision(RevisionInternal rev, String prevRevID) throws CouchbaseLiteException {

        Map<String, Object> revAttachments = (Map<String, Object>) rev.getPropertyForKey("_attachments");
        if (revAttachments == null || revAttachments.size() == 0 || rev.isDeleted()) {
            return new HashMap<String, AttachmentInternal>();
        }

        Map<String, AttachmentInternal> attachments = new HashMap<String, AttachmentInternal>();
        for (String name : revAttachments.keySet()) {
            Map<String, Object> attachInfo = (Map<String, Object>) revAttachments.get(name);
            String contentType = (String) attachInfo.get("content_type");
            AttachmentInternal attachment = new AttachmentInternal(name, contentType);
            String newContentBase64 = (String) attachInfo.get("data");
            if (newContentBase64 != null) {
                // If there's inline attachment data, decode and store it:
                byte[] newContents;
                try {
                    newContents = Base64.decode(newContentBase64, Base64.DONT_GUNZIP);
                } catch (IOException e) {
                    throw new CouchbaseLiteException(e, Status.BAD_ENCODING);
                }
                attachment.setLength(newContents.length);
                BlobKey outBlobKey = new BlobKey();
                boolean storedBlob = getAttachments().storeBlob(newContents, outBlobKey);
                attachment.setBlobKey(outBlobKey);
                if (!storedBlob) {
                    throw new CouchbaseLiteException(Status.STATUS_ATTACHMENT_ERROR);
                }
            }
            else if (attachInfo.containsKey("follows") && ((Boolean)attachInfo.get("follows")).booleanValue() == true) {
                // "follows" means the uploader provided the attachment in a separate MIME part.
                // This means it's already been registered in _pendingAttachmentsByDigest;
                // I just need to look it up by its "digest" property and install it into the store:
                installAttachment(attachment, attachInfo);
            }
            else {
                // This item is just a stub; validate and skip it
                if (((Boolean) attachInfo.get("stub")).booleanValue() == false) {
                    throw new CouchbaseLiteException("Expected this attachment to be a stub", Status.BAD_ATTACHMENT);
                }
                int revPos = ((Integer) attachInfo.get("revpos")).intValue();
                if (revPos <= 0) {
                    throw new CouchbaseLiteException("Invalid revpos: " + revPos, Status.BAD_ATTACHMENT);
                }
                Map<String, Object> parentAttachments = getAttachments(rev.getDocId(), prevRevID);
                if (parentAttachments != null && parentAttachments.containsKey(name)) {
                    Map<String, Object> parentAttachment = (Map<String, Object>) parentAttachments.get(name);
                    try {
                        BlobKey blobKey = new BlobKey((String) attachInfo.get("digest"));
                        attachment.setBlobKey(blobKey);
                    } catch (IllegalArgumentException e) {
                        continue;
                    }
                } else if (parentAttachments == null || !parentAttachments.containsKey(name)) {
                    BlobKey blobKey = null;
                    try {
                        blobKey = new BlobKey((String) attachInfo.get("digest"));
                    } catch (IllegalArgumentException e) {
                        continue;
                    }
                    if (getAttachments().hasBlobForKey(blobKey)) {
                        attachment.setBlobKey(blobKey);
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            // Handle encoded attachment:
            String encodingStr = (String) attachInfo.get("encoding");
            if (encodingStr != null && encodingStr.length() > 0) {
                if (encodingStr.equalsIgnoreCase("gzip")) {
                    attachment.setEncoding(AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP);
                }
                else {
                    throw new CouchbaseLiteException("Unnkown encoding: " + encodingStr, Status.BAD_ENCODING);
                }
                attachment.setEncodedLength(attachment.getLength());
                if (attachInfo.containsKey("length")) {
                    Number attachmentLength = (Number) attachInfo.get("length");
                    attachment.setLength(attachmentLength.longValue());
                }
            }
            if (attachInfo.containsKey("revpos")) {
                attachment.setRevpos((Integer)attachInfo.get("revpos"));
            }

            attachments.put(name, attachment);
        }

        return attachments;

    }

    /**
     * Inserts an already-existing revision replicated from a remote sqliteDb.
     *
     * It must already have a revision ID. This may create a conflict! The revision's history must be given; ancestor revision IDs that don't already exist locally will create phantom revisions with no content.
     * @exclude
     * in CBLDatabase+Insertion.m
     * - (CBLStatus) forceInsert: (CBL_Revision*)inRev
     *           revisionHistory: (NSArray*)history  // in *reverse* order, starting with rev's revID
     *                    source: (NSURL*)source
     */
    @InterfaceAudience.Private
    public void forceInsert(RevisionInternal rev, List<String> revHistory, URL source) throws CouchbaseLiteException {

        // TODO: in the iOS version, it is passed an immutable RevisionInternal and then
        // TODO: creates a mutable copy.  We should do the same here.
        // TODO: see github.com/couchbase/couchbase-lite-java-core/issues/206#issuecomment-44364624

        RevisionInternal winningRev = null;
        boolean inConflict = false;

        String docId = rev.getDocId();
        String revId = rev.getRevId();
        if (!isValidDocumentId(docId) || (revId == null)) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        int historyCount = 0;
        if (revHistory != null) {
            historyCount = revHistory.size();
        }
        if (historyCount == 0) {
            revHistory = new ArrayList<String>();
            revHistory.add(revId);
            historyCount = 1;
        } else if (!revHistory.get(0).equals(rev.getRevId())) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        boolean success = false;
        beginTransaction();
        try {
            // First look up the document's row-id and all locally-known revisions of it:
            Map<String, RevisionInternal> localRevs = null;
            boolean isNewDoc = (historyCount == 1);
            long docNumericID = getOrInsertDocNumericID(docId);
            RevisionList localRevsList = null;
            if (!isNewDoc) {
                localRevsList = getAllRevisionsOfDocumentID(docId, docNumericID, false);
                if (localRevsList == null) {
                    throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                }
                localRevs = new HashMap<String, RevisionInternal>();
                for (RevisionInternal r : localRevsList) {
                    localRevs.put(r.getRevId(), r);
                }
            }

            // Validate against the latest common ancestor:
            if (validations != null && validations.size() > 0) {
                RevisionInternal oldRev = null;
                for (int i = 1; i < historyCount; i++) {
                    oldRev = (localRevs != null) ? localRevs.get(revHistory.get(i)) : null;
                    if (oldRev != null) {
                        break;
                    }
                }
                String parentRevId = (historyCount > 1) ? revHistory.get(1) : null;
                validateRevision(rev, oldRev, parentRevId);
            }

            AtomicBoolean outIsDeleted = new AtomicBoolean(false);
            AtomicBoolean outIsConflict = new AtomicBoolean(false);
            boolean oldWinnerWasDeletion = false;
            String oldWinningRevID = winningRevIDOfDoc(docNumericID, outIsDeleted, outIsConflict);
            if (outIsDeleted.get()) {
                oldWinnerWasDeletion = true;
            }
            if (outIsConflict.get()) {
                inConflict = true;
            }

            // Walk through the remote history in chronological order, matching each revision ID to
            // a local revision. When the list diverges, start creating blank local revisions to fill
            // in the local history:
            long sequence = 0;
            long localParentSequence = 0;
            for (int i = revHistory.size() - 1; i >= 0; --i) {
                revId = revHistory.get(i);
                RevisionInternal localRev = (localRevs != null) ? localRevs.get(revId) : null;
                if (localRev != null) {
                    // This revision is known locally. Remember its sequence as the parent of the next one:
                    sequence = localRev.getSequence();
                    assert (sequence > 0);
                    localParentSequence = sequence;
                } else {
                    // This revision isn't known, so add it:

                    RevisionInternal newRev;
                    byte[] data = null;
                    boolean current = false;
                    if (i == 0) {
                        // Hey, this is the leaf revision we're inserting:
                        newRev = rev;
                        if (!rev.isDeleted()) {
                            data = encodeDocumentJSON(rev);
                            if (data == null) {
                                throw new CouchbaseLiteException(Status.BAD_REQUEST);
                            }
                        }
                        current = true;
                    } else {
                        // It's an intermediate parent, so insert a stub:
                        newRev = new RevisionInternal(docId, revId, false);
                    }

                    // Insert it:
                    sequence = insertRevision(newRev, docNumericID, sequence, current, (newRev.getAttachments() != null && newRev.getAttachments().size() > 0), data);

                    if (sequence <= 0) {
                        throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                    }

                    if (i == 0) {
                        // Write any changed attachments for the new revision. As the parent sequence use
                        // the latest local revision (this is to copy attachments from):
                        String prevRevID = (revHistory.size() >= 2) ? revHistory.get(1) : null;
                        Map<String, AttachmentInternal> attachments = getAttachmentsFromRevision(rev, prevRevID);
                        if (attachments != null) {
                            processAttachmentsForRevision(attachments, rev, localParentSequence, localRevsList);
                            stubOutAttachmentsInRevision(attachments, rev);
                        }
                    }

                }
            }

            // Mark the latest local rev as no longer current:
            if (localParentSequence > 0 && localParentSequence != sequence) {
                ContentValues args = new ContentValues();
                args.put("current", 0);
                String[] whereArgs = {Long.toString(localParentSequence)};
                int numRowsChanged = 0;
                try {
                    numRowsChanged = database.update("revs", args, "sequence=? AND current!=0", whereArgs);
                    if (numRowsChanged == 0) {
                        inConflict = true;  // local parent wasn't a leaf, ergo we just created a branch
                    }
                } catch (SQLException e) {
                    throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                }
            }

            winningRev = winner(docNumericID, oldWinningRevID, oldWinnerWasDeletion, rev);

            success = true;

            // Notify and return:
            databaseStorageChanged(new DocumentChange(rev, winningRev, inConflict, source));

        } catch (SQLException e) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            endTransaction(success);
        }
    }

    /** VALIDATION **/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void validateRevision(RevisionInternal newRev, RevisionInternal oldRev, String parentRevID) throws CouchbaseLiteException {
        if(validations == null || validations.size() == 0) {
            return;
        }

        SavedRevision publicRev = new SavedRevision(this, newRev);
        publicRev.setParentRevisionID(parentRevID);

        ValidationContextImpl context = new ValidationContextImpl(this, oldRev, newRev);

        for (String validationName : validations.keySet()) {
            Validator validation = getValidation(validationName);
            validation.validate(publicRev, context);
            if(context.getRejectMessage() != null) {
                throw new CouchbaseLiteException(context.getRejectMessage(), Status.FORBIDDEN);
            }
        }
    }

    /*************************************************************************************************/
    /*** Database+Replication                                                                    ***/
    /*************************************************************************************************/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Replication getActiveReplicator(URL remote, boolean push) {
        if(activeReplicators != null) {
            synchronized (activeReplicators) {
                for (Replication replicator : activeReplicators) {
                    if(replicator.getRemoteUrl().equals(remote) && replicator.isPull() == !push && replicator.isRunning()) {
                        return replicator;
                    }
                }
            }
        }
        return null;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Replication getReplicator(URL remote, boolean push, boolean continuous, ScheduledExecutorService workExecutor) {
        Replication replicator = getReplicator(remote, null, push, continuous, workExecutor);

    	return replicator;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Replication getReplicator(String sessionId) {
        if (allReplicators != null) {
            synchronized (allReplicators) {
                for (Replication replicator : allReplicators) {
                    if (replicator.getSessionID().equals(sessionId)) {
                        return replicator;
                    }
                }
            }
        }
        return null;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Replication getReplicator(URL remote, HttpClientFactory httpClientFactory, boolean push, boolean continuous, ScheduledExecutorService workExecutor) {
        Replication result = getActiveReplicator(remote, push);
        if(result != null) {
            return result;
        }
        if (push) {
            result = new Replication(this, remote, Replication.Direction.PUSH, httpClientFactory, workExecutor);
        } else {
            result = new Replication(this, remote, Replication.Direction.PULL, httpClientFactory, workExecutor);
        }
        result.setContinuous(continuous);
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String lastSequenceWithCheckpointId(String checkpointId) {
        Cursor cursor = null;
        String result = null;
        try {
            // This table schema is out of date but I'm keeping it the way it is for compatibility.
            // The 'remote' column now stores the opaque checkpoint IDs, and 'push' is ignored.
            String[] args = { checkpointId };
            cursor = database.rawQuery("SELECT last_sequence FROM replicators WHERE remote=?", args);
            if(cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting last sequence", e);
            return null;
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public boolean setLastSequence(String lastSequence, String checkpointId, boolean push) {
        Log.v(Database.TAG, "%s: setLastSequence() called with lastSequence: %s checkpointId: %s", this, lastSequence, checkpointId);
        ContentValues values = new ContentValues();
        values.put("remote", checkpointId);
        values.put("push", push);
        values.put("last_sequence", lastSequence);
        long newId = database.insertWithOnConflict("replicators", null, values, SQLiteStorageEngine.CONFLICT_REPLACE);
        return (newId == -1);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getLastSequenceStored(String checkpointId, boolean push) {

        if (!push) {
            throw new RuntimeException("need to unhardcode push = 1 before it will work with pull replications");
        }

        String sql = "SELECT last_sequence FROM replicators "
                + "WHERE remote = ? AND push = 1 ";
        String[] args = {checkpointId};
        Cursor cursor = null;
        RevisionList changes = null;
        String lastSequence = null;

        try {

            cursor = database.rawQuery(sql, args);
            cursor.moveToNext();
            while(!cursor.isAfterLast()) {
                lastSequence = cursor.getString(0);
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error", e);
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

        return lastSequence;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static String quote(String string) {
        return string.replace("'", "''");
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static String joinQuotedObjects(List<Object> objects) {
        List<String> strings = new ArrayList<String>();
        for (Object object : objects) {
            strings.add(object != null ? object.toString() : null);
        }
        return joinQuoted(strings);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static String joinQuoted(List<String> strings) {
        if(strings.size() == 0) {
            return "";
        }

        String result = "'";
        boolean first = true;
        for (String string : strings) {
            if(first) {
                first = false;
            }
            else {
                result = result + "','";
            }
            result = result + quote(string);
        }
        result = result + "'";

        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public int findMissingRevisions(RevisionList touchRevs) throws SQLException {
        int numRevisionsRemoved = 0;
        if(touchRevs.size() == 0) {
            return numRevisionsRemoved;
        }

        String quotedDocIds = joinQuoted(touchRevs.getAllDocIds());
        String quotedRevIds = joinQuoted(touchRevs.getAllRevIds());

        String sql = "SELECT docid, revid FROM revs, docs " +
                      "WHERE docid IN (" +
                      quotedDocIds +
                      ") AND revid in (" +
                      quotedRevIds + ")" +
                      " AND revs.doc_id == docs.doc_id";

        Cursor cursor = null;
        try {
            cursor = database.rawQuery(sql, null);
            cursor.moveToNext();
            while(!cursor.isAfterLast()) {
                RevisionInternal rev = touchRevs.revWithDocIdAndRevId(cursor.getString(0), cursor.getString(1));

                if(rev != null) {
                    touchRevs.remove(rev);
                    numRevisionsRemoved += 1;
                }

                cursor.moveToNext();
            }
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return numRevisionsRemoved;
    }

    /*************************************************************************************************/
    /*** Database+LocalDocs                                                                      ***/
    /*************************************************************************************************/

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    static String makeLocalDocumentId(String documentId) {
        return String.format("_local/%s", documentId);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionInternal putLocalRevision(RevisionInternal revision, String prevRevID) throws CouchbaseLiteException {
        String docID = revision.getDocId();
        if(!docID.startsWith("_local/")) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        if(!revision.isDeleted()) {
            // PUT:
            byte[] json = encodeDocumentJSON(revision);
            String newRevID;
            if(prevRevID != null) {
                int generation = RevisionInternal.generationFromRevID(prevRevID);
                if(generation == 0) {
                    throw new CouchbaseLiteException(Status.BAD_REQUEST);
                }
                newRevID = Integer.toString(++generation) + "-local";
                ContentValues values = new ContentValues();
                values.put("revid", newRevID);
                values.put("json", json);
                String[] whereArgs = { docID, prevRevID };
                try {
                    int rowsUpdated = database.update("localdocs", values, "docid=? AND revid=?", whereArgs);
                    if(rowsUpdated == 0) {
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
                    database.insertWithOnConflict("localdocs", null, values, SQLiteStorageEngine.CONFLICT_IGNORE);
                } catch (SQLException e) {
                    throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
                }
            }
            return revision.copyWithDocID(docID, newRevID);
        }
        else {
            // DELETE:
            deleteLocalDocument(docID, prevRevID);
            return revision;
        }
    }


    /**
     * Creates a one-shot query with the given map function. This is equivalent to creating an
     * anonymous View and then deleting it immediately after querying it. It may be useful during
     * development, but in general this is inefficient if this map will be used more than once,
     * because the entire view has to be regenerated from scratch every time.
     * @exclude
     */
    @InterfaceAudience.Private
    public Query slowQuery(Mapper map) {
        return new Query(this, map);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    RevisionInternal getParentRevision(RevisionInternal rev) {

        // First get the parent's sequence:
        long seq = rev.getSequence();
        if (seq > 0) {
            seq = longForQuery("SELECT parent FROM revs WHERE sequence=?", new String[] { Long.toString(seq) });
        } else {
            long docNumericID = getDocNumericID(rev.getDocId());
            if (docNumericID <= 0) {
                return null;
            }
            String[] args = new String[] { Long.toString(docNumericID), rev.getRevId() } ;
            seq = longForQuery("SELECT parent FROM revs WHERE doc_id=? and revid=?", args);
        }

        if (seq == 0) {
            return null;
        }

        // Now get its revID and deletion status:
        RevisionInternal result = null;

        String[] args = { Long.toString(seq) };
        String queryString = "SELECT revid, deleted FROM revs WHERE sequence=?";
        Cursor cursor = null;

        try {
            cursor = database.rawQuery(queryString, args);
            if (cursor.moveToNext()) {
                String revId = cursor.getString(0);
                boolean deleted = (cursor.getInt(1) > 0);
                result = new RevisionInternal(rev.getDocId(), revId, deleted/*, this*/);
                result.setSequence(seq);
            }
        } finally {
            cursor.close();
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    long longForQuery(String sqlQuery, String[] args) throws SQLException {
        Cursor cursor = null;
        long result = 0;
        try {
            cursor = database.rawQuery(sqlQuery, args);
            if(cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * Purges specific revisions, which deletes them completely from the local database _without_ adding a "tombstone" revision. It's as though they were never there.
     * This operation is described here: http://wiki.apache.org/couchdb/Purge_Documents
     * @param docsToRevs  A dictionary mapping document IDs to arrays of revision IDs.
     * @resultOn success will point to an NSDictionary with the same form as docsToRev, containing the doc/revision IDs that were actually removed.
     * @exclude
     */
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
                            database.execSQL("DELETE FROM revs WHERE doc_id=?", args);
                        } catch (SQLException e) {
                            Log.e(Database.TAG, "Error deleting revisions", e);
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
                            cursor = database.rawQuery(queryString, args);
                            if (!cursor.moveToNext()) {
                                Log.w(Database.TAG, "No results for query: %s", queryString);
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
                            Log.i(Database.TAG, "Purging doc '%s' revs (%s); asked for (%s)", docID, revsToPurge, revIDs);
                            if (seqsToPurge.size() > 0) {
                                // Now delete the sequences to be purged.
                                String seqsToPurgeList = TextUtils.join(",", seqsToPurge);
                                String sql = String.format("DELETE FROM revs WHERE sequence in (%s)", seqsToPurgeList);
                                try {
                                    database.execSQL(sql);
                                } catch (SQLException e) {
                                    Log.e(Database.TAG, "Error deleting revisions via: " + sql, e);
                                    return false;
                                }
                            }
                            revsPurged.addAll(revsToPurge);

                        } catch (SQLException e) {
                            Log.e(Database.TAG, "Error getting revisions", e);
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

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected boolean replaceUUIDs() {
        String query = "UPDATE INFO SET value='"+ Misc.TDCreateUUID()+"' where key = 'privateUUID';";
        try {
            database.execSQL(query);
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error updating UUIDs", e);
            return false;
        }
        query = "UPDATE INFO SET value='"+ Misc.TDCreateUUID()+"' where key = 'publicUUID';";
        try {
            database.execSQL(query);
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error updating UUIDs", e);
            return false;
        }
        return true;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionInternal getLocalDocument(String docID, String revID) {
        // docID already should contain "_local/" prefix
        RevisionInternal result = null;
        Cursor cursor = null;
        try {
            String[] args = { docID };
            cursor = database.rawQuery("SELECT revid, json FROM localdocs WHERE docid=?", args);
            if(cursor.moveToNext()) {
                String gotRevID = cursor.getString(0);
                if(revID != null && (!revID.equals(gotRevID))) {
                    return null;
                }
                byte[] json = cursor.getBlob(1);
                Map<String,Object> properties = null;
                try {
                    properties = Manager.getObjectMapper().readValue(json, Map.class);
                    properties.put("_id", docID);
                    properties.put("_rev", gotRevID);
                    result = new RevisionInternal(docID, gotRevID, false);
                    result.setProperties(properties);
                } catch (Exception e) {
                    Log.w(Database.TAG, "Error parsing local doc JSON", e);
                    return null;
                }

            }
            return result;
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting local document", e);
            return null;
        } finally {
            if(cursor != null) {
                cursor.close();
            }
        }

    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public long getStartTime() {
        return this.startTime;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void deleteLocalDocument(String docID, String revID) throws CouchbaseLiteException {
        if(docID == null) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }
        if(revID == null) {
            // Didn't specify a revision to delete: 404 or a 409, depending
            if (getLocalDocument(docID, null) != null) {
                throw new CouchbaseLiteException(Status.CONFLICT);
            }
            else {
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            }
        }
        String[] whereArgs = { docID, revID };
        try {
            int rowsDeleted = database.delete("localdocs", "docid=? AND revid=?", whereArgs);
            if(rowsDeleted == 0) {
                if (getLocalDocument(docID, null) != null) {
                    throw new CouchbaseLiteException(Status.CONFLICT);
                }
                else {
                    throw new CouchbaseLiteException(Status.NOT_FOUND);
                }
            }
        } catch (SQLException e) {
            throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Set the database's name.
     * @exclude
     */
    @InterfaceAudience.Private
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Prune revisions to the given max depth.  Eg, remove revisions older than that max depth,
     * which will reduce storage requirements.
     *
     * TODO: This implementation is a bit simplistic. It won't do quite the right thing in
     * histories with branches, if one branch stops much earlier than another. The shorter branch
     * will be deleted entirely except for its leaf revision. A more accurate pruning
     * would require an expensive full tree traversal. Hopefully this way is good enough.
     *
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Private
    /* package */ int pruneRevsToMaxDepth(int maxDepth) throws CouchbaseLiteException {

        int outPruned = 0;
        boolean shouldCommit = false;
        Map<Long, Integer> toPrune = new HashMap<Long, Integer>();

        if (maxDepth == 0) {
            maxDepth = getMaxRevTreeDepth();
        }

        // First find which docs need pruning, and by how much:

        Cursor cursor = null;
        String[] args = { };

        long docNumericID = -1;
        int minGen = 0;
        int maxGen = 0;

        try {

            cursor = database.rawQuery("SELECT doc_id, MIN(revid), MAX(revid) FROM revs GROUP BY doc_id", args);

            while(cursor.moveToNext()) {
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
                String[] deleteArgs = { Long.toString(docNumericID), minIDToKeep};
                int rowsDeleted = database.delete("revs", "doc_id=? AND revid < ? AND current=0", deleteArgs);
                outPruned += rowsDeleted;
            }

            shouldCommit = true;

        } catch (Exception e) {
            throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
        } finally {
            endTransaction(shouldCommit);
            if(cursor != null) {
                cursor.close();
            }
        }

        return outPruned;

    }


    /**
     * Is the database open?
     * @exclude
     */
    @InterfaceAudience.Private
    public boolean isOpen() {
        return open;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void addReplication(Replication replication) {
        if (allReplicators != null) {
            allReplicators.add(replication);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void forgetReplication(Replication replication) {
        allReplicators.remove(replication);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void addActiveReplication(Replication replication) {

        replication.addChangeListener(new Replication.ChangeListener() {
            @Override
            public void changed(Replication.ChangeEvent event) {
                if (event.getTransition() != null && event.getTransition().getDestination() == ReplicationState.STOPPED) {
                    if (activeReplicators != null) {
                        activeReplicators.remove(event.getSource());
                    }
                }
            }
        });

        if (activeReplicators != null) {
            activeReplicators.add(replication);
        }


    }

    /**
     * Get the PersistentCookieStore associated with this database.
     * Will lazily create one if none exists.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public PersistentCookieStore getPersistentCookieStore() {

        if (persistentCookieStore == null) {
            persistentCookieStore = new PersistentCookieStore(this);
        }
        return persistentCookieStore;
    }

    /**
     * Check if Write-Ahead Logging (WAL) available
     * http://sqlite.org/wal.html (WAL)
     * https://www.sqlite.org/c3ref/c_source_id.html (SQLite Version)
     * WAL requires 3.7.0 or higher
     */
    protected boolean isWALAvailable() {
        String version = getSQLiteVersion();
        if (version.isEmpty())
            return false;

        int major = 0;
        int minor = 0;
        int i = 0;
        StringTokenizer tok = new StringTokenizer(version, ".");
        while (tok.hasMoreTokens()) {
            String token = tok.nextToken();
            switch (i) {
                case 0:
                    major = Integer.parseInt(token);
                    break;
                case 1:
                    minor = Integer.parseInt(token);
                    break;
            }
            i++;
        }

        if (major >= 4)
            return true;
        else if (major == 3 && minor >= 7)
            return true;

        return false;
    }

    /**
     * Get SQLite version
     */
    protected String getSQLiteVersion() {
        String sql = "select sqlite_version() AS sqlite_version";
        Cursor cursor = null;
        String version = "";
        try {
            cursor = database.rawQuery(sql, null);
            if (cursor.moveToNext()) {
                version += cursor.getString(0);
            }
        } catch (SQLException e) {
            Log.e(Database.TAG, "Error getting SQLite version", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return version;
    }

    /**
     * https://github.com/couchbase/couchbase-lite-ios/issues/615
     */
    protected void optimizeSQLIndexes() {
        Log.v(Log.TAG_DATABASE, "calls optimizeSQLIndexes()");
        final long currentSequence = getLastSequenceNumber();
        if (currentSequence > 0) {
            final long lastOptimized = getLastOptimized();
            if (lastOptimized <= currentSequence / 10) {
                runInTransaction(new TransactionalTask() {
                    @Override
                    public boolean run() {
                        Log.i(Log.TAG_DATABASE, "%s: Optimizing SQL indexes (curSeq=%d, last run at %d)", this, currentSequence, lastOptimized);
                        database.execSQL("ANALYZE");
                        database.execSQL("ANALYZE sqlite_master");
                        setInfo("last_optimized", String.valueOf(currentSequence));
                        return true;
                    }
                });
            }
        }
    }

    private long getLastOptimized() {
        String info = infoForKey("last_optimized");
        if (info != null) {
            return Long.parseLong(info);
        }
        return 0;
    }
}
