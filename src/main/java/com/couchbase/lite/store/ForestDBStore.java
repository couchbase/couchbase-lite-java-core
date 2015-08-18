package com.couchbase.lite.store;

import com.couchbase.lite.BlobKey;
import com.couchbase.lite.ChangesOptions;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.DocumentChange;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Misc;
import com.couchbase.lite.QueryOptions;
import com.couchbase.lite.ReplicationFilter;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.TransactionalTask;
import com.couchbase.lite.cbforest.Config;
import com.couchbase.lite.cbforest.ContentOptions;
import com.couchbase.lite.cbforest.Database;
import com.couchbase.lite.cbforest.DocEnumerator;
import com.couchbase.lite.cbforest.Document;
import com.couchbase.lite.cbforest.KeyStore;
import com.couchbase.lite.cbforest.KeyStoreWriter;
import com.couchbase.lite.cbforest.OpenFlags;
import com.couchbase.lite.cbforest.RevID;
import com.couchbase.lite.cbforest.RevIDBuffer;
import com.couchbase.lite.cbforest.Revision;
import com.couchbase.lite.cbforest.Slice;
import com.couchbase.lite.cbforest.Transaction;
import com.couchbase.lite.cbforest.VectorRevID;
import com.couchbase.lite.cbforest.VectorRevision;
import com.couchbase.lite.cbforest.VersionedDocument;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.RevisionUtils;
import com.couchbase.lite.util.Log;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hideki on 8/13/15.
 */
public class ForestDBStore implements Store {

    public static final String TAG = ForestDBStore.class.getSimpleName();

    /** static constructor */
    static {
        try {
            System.loadLibrary("cbforest");
        } catch (Exception e) {
            Log.e(TAG, "ERROR: Failed to load libcbforest");
        }
    }

    public static String kDBFilename = "db.forest";

    // Size of ForestDB buffer cache allocated for a database
    private static final BigInteger kDBBufferCacheSize = new BigInteger("8388608");

    // ForestDB Write-Ahead Log size (# of records)
    private static final BigInteger kDBWALThreshold = new BigInteger("1024");

    // How often ForestDB should check whether databases need auto-compaction
    private static final BigInteger kAutoCompactInterval = new BigInteger("300");

    private static final int kDefaultMaxRevTreeDepth = 20;

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
    private Database forest;
    private Transaction forestTransaction = null;
    private TransactionLevel transactionLevel;
    private StoreDelegate delegate;
    private int maxRevTreeDepth;
    private boolean autoCompact;
    private boolean readOnly = false;

    ///////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////

    public ForestDBStore(String directory, Manager manager, StoreDelegate delegate) {
        Log.w(TAG, "ForestDBStore()");
        assert (new File(directory).isAbsolute()); // path must be absolute
        this.directory = directory;
        File dir = new File(directory);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("directory '" + directory + "' does not exist or not directory");
        }
        this.path = new File(directory, kDBFilename).getPath();
        this.manager = manager;
        this.forest = null;
        this.transactionLevel = new TransactionLevel();
        this.delegate = delegate;
        this.autoCompact = true;
        this.maxRevTreeDepth = kDefaultMaxRevTreeDepth;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Implementation of Storage
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZATION AND CONFIGURATION:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public boolean databaseExists(String directory) {
        Log.w(TAG, "databaseExists()");
        return new File(directory, kDBFilename).exists();
    }

    @Override
    public boolean open() {
        Log.w(TAG, "open()");
        OpenFlags options = readOnly ? OpenFlags.FDB_OPEN_FLAG_RDONLY : OpenFlags.FDB_OPEN_FLAG_CREATE;

        Config config = Database.defaultConfig();
        config.setFlags(options);
        config.setBuffercacheSize(kDBBufferCacheSize);
        config.setWalThreshold(kDBWALThreshold);
        config.setWalFlushBeforeCommit(true);
        //config.seqtree_opt = true;
        config.setCompressDocumentBody(true);
        if (autoCompact)
            config.setCompactorSleepDuration(kAutoCompactInterval);
        else
            config.setCompactionThreshold((short) 0);

        try {
            forest = new Database(path, config);
        } catch (Exception e) {
            Log.e(TAG, "Failed to open the forestdb: code=%s", e.getMessage());
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public void close() {
        Log.w(TAG, "close()");
        if (forest != null) {
            forest.delete();
            forest = null;
        }
        transactionLevel.set(0);
    }

    @Override
    public void setDelegate(StoreDelegate delegate) {
        Log.w(TAG, "setDelegate()");
        this.delegate = delegate;
    }

    @Override
    public StoreDelegate getDelegate() {
        Log.w(TAG, "getDelegate()");
        return delegate;
    }

    @Override
    public void setMaxRevTreeDepth(int maxRevTreeDepth) {
        Log.w(TAG, "setMaxRevTreeDepth()");
        this.maxRevTreeDepth = maxRevTreeDepth;
    }

    @Override
    public int getMaxRevTreeDepth() {
        Log.w(TAG, "getMaxRevTreeDepth()");
        return maxRevTreeDepth;
    }

    ///////////////////////////////////////////////////////////////////////////
    // DATABASE ATTRIBUTES & OPERATIONS:
    ///////////////////////////////////////////////////////////////////////////

    // #pragma mark - INFO FOR KEY:

    @Override
    public long setInfo(String key, String info) {
        Log.w(TAG, "setInfo() key="+key + ", info="+info);
        final String k = key;
        final String i = info;
        try {
            final KeyStore infoStore = new KeyStore(forest, "info");
            runInTransaction(new TransactionalTask() {
                @Override
                public boolean run() {
                    KeyStoreWriter infoWriter = forestTransaction.toKeyStoreWriter(infoStore);
                    try {
                        infoWriter.set(new Slice(k.getBytes()), new Slice(i.getBytes()));
                    } catch (Exception e) {
                        Log.e(TAG, "KeyStoreWriter.set() error="+e.getMessage());
                        return false;
                    }
                    infoWriter.delete();
                    return true;
                }
            });
            infoStore.delete();
        }
        catch (Exception e){
            Log.e(TAG, "Error in setInfo(): "+ e.getMessage(), e);
            return Status.UNKNOWN;
        }
        return Status.OK;
    }

    @Override
    public String getInfo(String key) {
        Log.w(TAG, "getInfo() key="+key);
        try {
            KeyStore infoStore = new KeyStore(forest, "info");
            Document doc = infoStore.get(new Slice(key.getBytes()));
            String value = null;
            if(doc != null && doc.getBody() != null && doc.getBody().getBuf()!= null)
                value = new String(doc.getBody().getBuf());
            infoStore.delete();
            Log.w(TAG, "getInfo() value=" + value);
            return value;
        }
        catch (Exception e){
            Log.e(TAG, "Error in getInfo(): "+ e.getMessage(), e);
            return null;
        }
    }

    @Override
    public int getDocumentCount() {
        Log.w(TAG, "getDocumentCount()");
        try {
            DocEnumerator.Options ops = new DocEnumerator.Options();
            ops.setContentOption(ContentOptions.kMetaOnly);
            int count = 0;
            DocEnumerator e = new DocEnumerator(forest, new Slice(), new Slice(), ops);
            for (; e.next(); ) {

                VersionedDocument vdoc = new VersionedDocument(forest, e.doc());
                if (!vdoc.isDeleted())
                    count++;
            }
            return count;
        } catch (Exception e) {
            Log.e(TAG, "Failed to getDocumentCount(): code=%s", e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public long getLastSequence() {
        Log.w(TAG, "getLastSequence()");
        try {
            return forest.getLastSequence().longValue();
        } catch (Exception e) {
            Log.e(TAG, "Failed to getLastSequence(): code=%s", e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public boolean inTransaction() {
        Log.w(TAG, "inTransaction()");
        return transactionLevel.get() > 0;
    }

    @Override
    public void compact() throws CouchbaseLiteException {
        Log.w(TAG, "compact()");
        try {
            forest.compact();
        } catch (Exception e) {
            String msg = String.format("Failed to compact(): code=%s", e.getMessage());
            Log.e(TAG, msg);
            e.printStackTrace();
            throw new CouchbaseLiteException(Status.UNKNOWN);
        }
    }

    @Override
    public boolean runInTransaction(TransactionalTask task) {
        Log.w(TAG, "runInTransaction()");
        beginTransaction();
        boolean commit = true;
        try {
            commit = task.run();
        } catch (Exception e) {
            commit = false;
            Log.e(TAG, e.toString(), e);
            throw new RuntimeException(e);
        } finally {
            endTransaction(commit);
        }
        return commit;
    }

    @Override
    public RevisionInternal getDocument(String docID, String revID, boolean withBody) {
        Log.w(TAG, "getDocument()");

        RevisionInternal result = null;

        // TODO: add VersionDocument(Database, String)
        VersionedDocument doc = new VersionedDocument(forest, new Slice(docID.getBytes()));
        if(!doc.exists()) {
            doc.delete();
            //throw new CouchbaseLiteException(Status.NOT_FOUND);
            return null;
        }

        if(revID == null){
            Revision rev = doc.currentRevision();
            if(rev == null || rev.isDeleted()) {
                //throw new CouchbaseLiteException(Status.DELETED);
                return null;
            }
            // TODO: add String getRevID()
            // TODO: revID is something wrong!!!!!
            RevID tmpRevID = rev.getRevID();
            byte [] buff = tmpRevID.getBuf();
            long bufSize = tmpRevID.getBufSize();
            long size = tmpRevID.getSize();
            revID = rev.getRevID().toString();
            Log.e(TAG, "[getDocument()] revID => " + revID + ", bufSize=" + bufSize + ", size=" + size);
        }

        try {
            result = ForestBridge.revisionObjectFromForestDoc(doc, revID, withBody);
        } catch (Exception e) {
            Log.e(TAG, "Error in ForestBridge.revisionObjectFromForestDoc(): error=%s", e.getMessage());
            return null;
        }
        if(result == null)
            //throw new CouchbaseLiteException(Status.NOT_FOUND);
            return null;

        return result;
    }

    @Override
    public RevisionInternal loadRevisionBody(RevisionInternal rev) throws CouchbaseLiteException {
        Log.w(TAG, "loadRevisionBody()");

        try {
            VersionedDocument doc = new VersionedDocument(forest, new Slice(rev.getDocID().getBytes()));
            if (doc == null || !doc.exists())
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            if (!ForestBridge.loadBodyOfRevisionObject(rev, doc))
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            return rev;
        }catch(Exception e){
            Log.e(TAG, "ERROR in loadRevisionBody()", e);
            throw new CouchbaseLiteException(Status.UNKNOWN);
        }
    }

    @Override
    public RevisionInternal getParentRevision(RevisionInternal rev) {
        Log.w(TAG, "getParentRevision()");

        if(rev.getDocID() == null || rev.getRevID() == null)
            return null;

        RevisionInternal parent = null;
        VersionedDocument doc = new VersionedDocument(forest, new Slice(rev.getDocID().getBytes()));
        if(doc != null) {
            Revision revNode = null;
            try {
                revNode = doc.get(new RevIDBuffer(new Slice(rev.getRevID().getBytes())));
            } catch (Exception e) {
                Log.e(TAG, "Error in getParentRevision()", e);
                return null;
            }
            if (revNode != null) {
                Revision parentRevision = revNode.getParent();
                if(parentRevision!=null){
                    String parentRevID = new String(parentRevision.getRevID().getBuf());
                    parent = new RevisionInternal(rev.getDocID(), parentRevID, parentRevision.isDeleted());
                }
                revNode.delete();
            }
            doc.delete();
        }
        return parent;
    }

    @Override
    public List<RevisionInternal> getRevisionHistory(RevisionInternal rev) {
        Log.w(TAG, "getRevisionHistory()");
        try {
            String docId = rev.getDocID();
            String revId = rev.getRevID();
            VersionedDocument doc = new VersionedDocument(forest, new Slice(docId.getBytes()));
            com.couchbase.lite.cbforest.Revision revision = doc.get(new RevIDBuffer(new Slice(revId.getBytes())));
            List<RevisionInternal> history = ForestBridge.getRevisionHistory(docId, revision);
            doc.delete();
            return history;
        }catch(Exception e){
            Log.e(TAG, "Error in getRevisionHistory() rev="+rev, e);
            return null;
        }
    }

    @Override
    public RevisionList getAllRevisions(String docID, boolean onlyCurrent) {
        Log.w(TAG, "getAllRevisions()");

        // TODO: add VersionDocument(Database, String)
        VersionedDocument doc = new VersionedDocument(forest, new Slice(docID.getBytes()));
        if(!doc.exists()) {
            doc.delete();
            //throw new CouchbaseLiteException(Status.NOT_FOUND);
            return null;
        }
        RevisionList revs = new RevisionList();

        VectorRevision revNodes = null;
        if(onlyCurrent)
            revNodes = doc.currentRevisions();
        else
            revNodes = doc.allRevisions();

        for(int i = 0; i < revNodes.size(); i++){
            com.couchbase.lite.cbforest.Revision revNode = revNodes.get(i);
            RevisionInternal rev = new RevisionInternal(docID, new String(revNode.getRevID().getBuf()), revNode.isDeleted());
            revs.add(rev);
        }

        return revs;
    }

    @Override
    public List<String> getPossibleAncestorRevisionIDs(RevisionInternal rev, int limit, AtomicBoolean onlyAttachments) {
        Log.e(TAG, "getPossibleAncestorRevisionIDs()");
        return null;
    }

    @Override
    public String findCommonAncestor(RevisionInternal rev, List<String> revIDs) {
        Log.e(TAG, "findCommonAncestor()");
        return null;
    }

    @Override
    public int findMissingRevisions(RevisionList touchRevs) {
        Log.e(TAG, "findMissingRevisions()");
        return 0;
    }

    @Override
    public Set<BlobKey> findAllAttachmentKeys() throws CouchbaseLiteException {
        Log.w(TAG, "findAllAttachmentKeys()");

        try {
            Set<BlobKey> keys = new HashSet<BlobKey>();
            DocEnumerator.Options options = new DocEnumerator.Options();
            options.setContentOption(ContentOptions.kMetaOnly);
            DocEnumerator e = new DocEnumerator(forest, new Slice(), new Slice(), options);
            for (; e.next(); ) {
                VersionedDocument doc = new VersionedDocument(forest, e.doc());
                if (!doc.hasAttachments() || (doc.isDeleted() && !doc.isConflicted()))
                    continue;
                doc.read();
                // Since db is assumed to have just been compacted, we know that non-current revisions
                // won't have any bodies. So only scan the current revs.
                VectorRevision revNodes = doc.currentRevisions();
                for (int i = 0; i < revNodes.size(); i++) {
                    Revision revNode = revNodes.get(i);
                    if (revNode.isActive() && revNode.hasAttachments()) {
                        Slice body = revNode.readBody();
                        if (body.getSize() > 0) {
                            Map<String, Object> docProperties = Manager.getObjectMapper().readValue(body.getBuf(), Map.class);
                            if (docProperties.containsKey("_attachments")) {
                                Map<String, Object> attachments = (Map<String, Object>) docProperties.get("_attachments");
                                Iterator<String> itr = attachments.keySet().iterator();
                                while (itr.hasNext()) {
                                    String name = itr.next();
                                    Map<String, Object> attachment = (Map<String, Object>) attachments.get(name);
                                    String digest = (String) attachment.get("digest");
                                    BlobKey key = new BlobKey(digest);
                                    keys.add(key);
                                }
                            }


                        }
                    }
                }
                revNodes.delete();
                doc.delete();
            }
            e.delete();
            return keys;
        }catch(Exception e){
            Log.e(TAG, "Error in findAllAttachmentKeys()", e);
            return null;
        }
    }

    @Override
    public Map<String, Object> getAllDocs(QueryOptions options) throws CouchbaseLiteException {
        Log.e(TAG, "getAllDocs()");
        return null;
    }

    @Override
    public RevisionList changesSince(long lastSequence, ChangesOptions options, ReplicationFilter filter, Map<String, Object> filterParams) {
        Log.w(TAG, "changesSince()");

        try {
            // http://wiki.apache.org/couchdb/HTTP_database_API#Changes
            // Translate options to ForestDB:
            if (options == null) {
                options = new ChangesOptions();
            }

            DocEnumerator.Options forestOpts = new DocEnumerator.Options();
            forestOpts.setLimit(options.getLimit());
            forestOpts.setInclusiveEnd(true);
            forestOpts.setIncludeDeleted(false);
            boolean withBody = (options.isIncludeDocs() || filter != null);
            if (!withBody)
                forestOpts.setContentOption(ContentOptions.kMetaOnly);

            RevisionList changes = new RevisionList();
            // TODO: DocEnumerator -> use long instead of BigInteger
            BigInteger start = BigInteger.valueOf(lastSequence + 1);
            BigInteger end = BigInteger.valueOf(Long.MAX_VALUE);
            DocEnumerator e = new DocEnumerator(forest, start, end, forestOpts);
            for (; e.next(); ) {
                VersionedDocument doc = new VersionedDocument(forest, e.doc());
                List<String> revIDs;
                if (options.isIncludeConflicts() && doc.isConflicted()) {
                    if (forestOpts.getContentOption() == ContentOptions.kMetaOnly)
                        doc.read();
                    revIDs = ForestBridge.getCurrentRevisionIDs(doc);
                } else {
                    revIDs = new ArrayList<String>();
                    revIDs.add(new String(doc.getRevID().getBuf()));
                }

                for (String revID : revIDs) {
                    Log.w(TAG, "[changesSince()] revID => " + revID);
                    RevisionInternal rev = ForestBridge.revisionObjectFromForestDoc(doc, revID, withBody);
                    if (filter == null || delegate.runFilter(filter, filterParams, rev)) {
                        if (!options.isIncludeDocs())
                            rev.setBody(null);
                        changes.add(rev);
                    }
                }
            }
            return changes;
        }catch(Exception e){
            Log.e(TAG, "Error in changesSince()", e);
            return null;
        }
    }


    ///////////////////////////////////////////////////////////////////////////
    // INSERTION / DELETION:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public RevisionInternal add(String inDocID,
                                String inPrevRevID,
                                Map<String, Object> properties,
                                boolean deleting,
                                boolean allowConflict,
                                StorageValidation validationBlock,
                                Status outStatus)
            throws CouchbaseLiteException {
        Log.w(TAG, "add()");

        if (outStatus != null)
            outStatus.setCode(Status.OK);

        if (forest.isReadOnly()) {
            throw new CouchbaseLiteException(Status.FORBIDDEN);
        }

        byte[] json;
        if (properties != null && properties.size() > 0) {
            json = RevisionUtils.asCanonicalJSON(properties);
            if (json == null)
                throw new CouchbaseLiteException(Status.BAD_JSON);
        } else {
            json = "{}".getBytes();
        }

        RevisionInternal putRev = null;
        DocumentChange change = null;

        beginTransaction();
        try {
            String docID = inDocID;
            String prevRevID = inPrevRevID;

            com.couchbase.lite.cbforest.Document rawDoc = new com.couchbase.lite.cbforest.Document();
            if (docID != null && !docID.isEmpty()) {
                // Read the doc from the database:
                rawDoc.setKey(new Slice(docID.getBytes()));
                try {
                    forest.read(rawDoc);
                } catch (Exception e) {
                    try{
                        throw new CouchbaseLiteException(Integer.parseInt(e.getMessage()));
                    }catch (NumberFormatException nfe){
                        throw new CouchbaseLiteException(Status.EXCEPTION);
                    }
                }
            } else {
                // Create new doc ID, and don't bother to read it since it's a new doc:
                docID = Misc.CreateUUID();
                rawDoc.setKey(new Slice(docID.getBytes()));
            }

            // Parse the document revision tree:
            VersionedDocument doc = new VersionedDocument(forest, rawDoc);
            com.couchbase.lite.cbforest.Revision revNode;

            if (prevRevID != null) {
                // Updating an existing revision; make sure it exists and is a leaf:
                // TODO -> add VersionDocument.get(String revID)
                //      -> or Efficiently pass RevID to VersionDocument.get(RevID)
                //revNode = doc.get(new RevID(inPrevRevID));
                Log.w(TAG, "[putDoc()] prevRevID => " + prevRevID);
                try {
                    revNode = doc.get(new RevIDBuffer(new Slice(inPrevRevID.getBytes())));
                } catch (Exception e) {
                    try{
                        throw new CouchbaseLiteException(Integer.parseInt(e.getMessage()));
                    }catch (NumberFormatException nfe){
                        throw new CouchbaseLiteException(Status.EXCEPTION);
                    }
                }
                if (revNode == null)
                    throw new CouchbaseLiteException(Status.NOT_FOUND);
                else if (!allowConflict && !revNode.isLeaf())
                    throw new CouchbaseLiteException(Status.CONFLICT);
            } else {
                // No parent revision given:
                if (deleting) {
                    // Didn't specify a revision to delete: NotFound or a Conflict, depending
                    throw new CouchbaseLiteException(doc.exists() ? Status.CONFLICT : Status.NOT_FOUND);
                }
                // If doc exists, current rev must be in a deleted state or there will be a conflict:
                revNode = doc.currentRevision();
                if (revNode != null) {
                    if (revNode.isDeleted()) {
                        // New rev will be child of the tombstone:
                        // (T0D0: Write a horror novel called "Child Of The Tombstone"!)
                        prevRevID = new String(revNode.getRevID().getBuf());
                    } else {
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    }
                }
            }

            // Compute the new revID. (Can't be done earlier because prevRevID may have changed.)
            String newRevID = delegate.generateRevID(json, deleting, prevRevID);
            if (newRevID == null)
                throw new CouchbaseLiteException(Status.BAD_ID); // invalid previous revID (no numeric prefix)

            putRev = new RevisionInternal(docID, newRevID, deleting);

            if (properties != null) {
                properties.put("_id", docID);
                properties.put("_rev", newRevID);
                putRev.setProperties(properties);
            }

            // Run any validation blocks:
            if (validationBlock != null) {
                // Fetch the previous revision and validate the new one against it:
                RevisionInternal prevRev = null;
                if (prevRevID != null)
                    prevRev = new RevisionInternal(docID, prevRevID, revNode.isDeleted());
                Status status = validationBlock.validate(putRev, prevRev, prevRevID);
                if (status.isError()) {
                    outStatus.setCode(status.getCode());
                    throw new CouchbaseLiteException(status);
                }
            }

            // Add the revision to the database:
            int status;
            RevIDBuffer newrevid = new RevIDBuffer(new Slice(newRevID.getBytes()));
            {
                // TODO - add new RevIDBuffer(String)
                // TODO - add RevTree.insert(String, String, boolean, boolean, RevID arg4, boolean)
                com.couchbase.lite.cbforest.Revision fdbRev = doc.insert(
                        newrevid,
                        new Slice(json),
                        deleting,
                        (putRev.getAttachments() != null),
                        revNode,
                        allowConflict);
                outStatus.setCode(doc.getLatestHttpStatus());
                if (fdbRev != null)
                    putRev.setSequence(fdbRev.getSequence().longValue());
                if (fdbRev == null && outStatus.isError())
                    throw new CouchbaseLiteException(outStatus);
            }
            boolean isWinner = saveForest(doc, newrevid, properties);
            putRev.setSequence(doc.getSequence().longValue());

            change = changeWithNewRevision(putRev, isWinner, doc, null);
        } finally {
            endTransaction(outStatus.isSuccessful());
        }

        if (change != null)
            delegate.databaseStorageChanged(change);

        return putRev;
    }

    @Override
    public void forceInsert(RevisionInternal inRev,
                            List<String> inHistory,
                            final StorageValidation validationBlock,
                            URL inSource)
            throws CouchbaseLiteException {

        Log.e(TAG, "forceInsert()");

        if (forest.isReadOnly())
            throw new CouchbaseLiteException(Status.FORBIDDEN);

        final byte[] json = inRev.getJson();
        if(json == null)
            throw new CouchbaseLiteException(Status.BAD_JSON);

        final RevisionInternal rev = inRev;
        final List<String> history = inHistory;
        final URL source = inSource;

        final DocumentChange[] change = new DocumentChange[1];
        Status status = inTransaction(new Task() {
            @Override
            public Status run() {
                // First get the CBForest doc:
                VersionedDocument doc = new VersionedDocument(forest, new Slice(rev.getDocID().getBytes()));

                // Add the revision & ancestry to the doc:
                VectorRevID historyVector = new VectorRevID();
                convertRevIDs(history, historyVector);
                int common = doc.insertHistory(historyVector,
                        new Slice(json),
                        rev.isDeleted(),
                        rev.getAttachments() != null);
                if(common < 0)
                    return new Status(Status.BAD_REQUEST); // generation numbers not in descending order
                else if(common == 0)
                    return new Status(Status.OK); // No-op: No new revisions were inserted.

                // Validate against the common ancestor:
                // Validate against the latest common ancestor:
                if (validationBlock != null) {
                    RevisionInternal prev = null;
                    if(common < history.size()){
                        RevID revID = historyVector.get(common);
                        Revision r = null;
                        try {
                            r = doc.get(revID);
                        } catch (Exception e) {
                            Log.e(TAG, "Error in forceInsert()", e);
                            return new Status(Status.UNKNOWN);
                        }
                        boolean deleted = r.isDeleted();
                        prev = new RevisionInternal(rev.getDocID(), history.get(common), deleted);
                    }
                    String parentRevID = (history.size() > 1) ? history.get(1) : null;
                    Status status = validationBlock.validate(rev, prev, parentRevID);
                    if (status.isError()) {
                        return status;
                    }
                }

                // Save updated doc back to the database:
                boolean isWinner = saveForest(doc, historyVector.get(0), rev.getProperties());
                rev.setSequence(doc.getSequence().longValue());
                change[0] = changeWithNewRevision(rev, isWinner, doc, source);

                return new Status(Status.OK);
            }
        });

        if (change[0] != null)
            delegate.databaseStorageChanged(change[0]);

        if(status.isError())
            throw new CouchbaseLiteException(status.getCode());
    }

    @Override
    public Map<String, Object> purgeRevisions(Map<String, List<String>> docsToRevs) {
        Log.e(TAG, "purgeRevisions()");
        return null;
    }

    @Override
    public ViewStore getViewStorage(String name, boolean create) {
        Log.e(TAG, "getViewStorage()");
        return null;
    }

    @Override
    public List<String> getAllViewNames() {
        Log.e(TAG, "getAllViewNames()");
        return null;
    }

    @Override
    public RevisionInternal getLocalDocument(String docID, String revID) {
        Log.w(TAG, "getLocalDocument()");

        if(docID == null|| !docID.startsWith("_local/"))
            return null;

        KeyStore localDocs = null;
        try {
            localDocs = new KeyStore(forest, "_local");
        } catch (Exception e) {
            Log.e(TAG, "error=%s", e.getMessage());
            return null;
        }
        Document doc = null;
        try {
            doc = localDocs.get(new Slice(docID.getBytes()));
        } catch (Exception e) {
            Log.e(TAG, "error=%s", e.getMessage());
            return null;
        }
        if(!doc.exists())
            return null;

        String gotRevID = new String(doc.getMeta().getBuf());
        if(revID!=null && !revID.equals(gotRevID))
            return null;

        Map<String,Object> properties = getDocProperties(doc);
        if(properties == null)
            return null;
        properties.put("_id", docID);
        properties.put("_rev", gotRevID);
        RevisionInternal result = new RevisionInternal(docID, gotRevID, false);
        result.setProperties(properties);
        return result;
    }

    @Override
    public RevisionInternal putLocalRevision(final RevisionInternal revision,
                                             final String prevRevID,
                                             final boolean obeyMVCC)
            throws CouchbaseLiteException {

        final String docID = revision.getDocID();
        if(!docID.startsWith("_local/")) {
            throw new CouchbaseLiteException(Status.BAD_ID);
        }

        if(revision.isDeleted()) {
            // DELETE:
            Status status = deleteLocalDocument(docID, prevRevID, obeyMVCC);
            if(status.isSuccessful())
                return revision;
            else
                throw new CouchbaseLiteException(status.getCode());
        }
        else {
            // PUT:
            final KeyStore localDocs;
            try {
                localDocs = new KeyStore(forest, "_local");
            } catch (Exception e) {
                Log.e(TAG, "Error in putLocalRevision()", e);
                throw new CouchbaseLiteException(Status.UNKNOWN);
            }
            final RevisionInternal[] result = new RevisionInternal[1];
            Status status = inTransaction(new Task() {
                @Override
                public Status run() {
                    try {
                        KeyStoreWriter localWriter = forestTransaction.toKeyStoreWriter(localDocs);
                        byte[] json = revision.getJson();
                        if (json == null)
                            return new Status(Status.BAD_JSON);
                        Slice key = new Slice(docID.getBytes());
                        //Document doc = localWriter.get(key);
                        Document doc = localDocs.get(key);
                        int generation = RevisionInternal.generationFromRevID(prevRevID);
                        if (obeyMVCC) {
                            if (prevRevID != null) {
                                if (!prevRevID.equals(new String(doc.getMeta().getBuf())))
                                    return new Status(Status.CONFLICT);
                                if (generation == 0)
                                    return new Status(Status.BAD_ID);
                            } else {
                                if (doc.exists())
                                    return new Status(Status.CONFLICT);
                            }
                        }
                        String newRevID = String.format("%d-local", ++generation);
                        localWriter.set(key, new Slice(newRevID.getBytes()), new Slice(json));
                        result[0] = revision.copyWithDocID(docID, newRevID);
                        return new Status(Status.CREATED);
                    }catch(Exception e){
                        Log.e(TAG, "Error in putLocalRevision()", e);
                        return new Status(Status.UNKNOWN);
                    }
                }
            });
            Log.e(TAG, "putLocalRevision() E");
            if(status.isSuccessful())
                return result[0];
            else
                throw new CouchbaseLiteException(status.getCode());
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal (PROTECTED & PRIVATE) METHODS
    ///////////////////////////////////////////////////////////////////////////

    private boolean saveForest(VersionedDocument doc, RevID revID, Map<String, Object> properties){
        // Is the new revision the winner?
        boolean isWinner = doc.currentRevision().getRevID().compare(revID) == 0;
        // Update the documentType:
        if(!isWinner)
            ;
        if (properties != null && properties.containsKey("type")) {
            String tmp = (String)properties.get("type");
            Slice type = new Slice(tmp.getBytes());
            doc.setDocType(type);
        }
        // Save:
        doc.prune(maxRevTreeDepth);
        doc.save(forestTransaction);
        return isWinner;
    }

    private DocumentChange changeWithNewRevision(RevisionInternal inRev,
                                                 boolean isWinningRev,
                                                 VersionedDocument doc,
                                                 URL source){
        String winningRevID;
        if(isWinningRev)
            winningRevID = inRev.getRevID();
        else{
            com.couchbase.lite.cbforest.Revision winningRevision = doc.currentRevision();
            winningRevID = new String(winningRevision.getRevID().getBuf());
        }
        return new DocumentChange(inRev, winningRevID, doc.hasConflict(), source);
    }

    private boolean beginTransaction() {
        Log.i(TAG, "BEGIN transaction...");
        transactionLevel.set(transactionLevel.get() + 1);
        if(transactionLevel.get() == 1)
            forestTransaction = new Transaction(forest);
        return true;
    }

    private boolean endTransaction(boolean commit) {
        Log.i(TAG, "END transaction");
        transactionLevel.set(transactionLevel.get() - 1);
        if(transactionLevel.get() == 0){
            if(!commit)
                forestTransaction.abort();
            forestTransaction.delete();
            forestTransaction = null;
            delegate.storageExitedTransaction(commit);
        }
        return true;
    }

    private static Map<String,Object> getDocProperties(Document doc){
        byte[] bodyData = doc.getBody().getBuf();
        if(bodyData == null)
            return null;
        try {
            return Manager.getObjectMapper().readValue(bodyData, Map.class);
        } catch (IOException e) {
            return null;
        }
    }
    /**
     * CBLDatabase+LocalDocs.m
     * - (CBLStatus) deleteLocalDocumentWithID: (NSString*)docID
     *                              revisionID: (NSString*)revID
     *                                obeyMVCC: (BOOL)obeyMVCC;
     */
    private Status deleteLocalDocument(final String inDocID, String inRevID, boolean obeyMVCC)
    {
        final String docID = inDocID;
        final String revID = inRevID;

        if (docID == null || !docID.startsWith("_local/"))
            return new Status(Status.BAD_ID);

        if (obeyMVCC && revID == null)
            // Didn't specify a revision to delete: 404 or a 409, depending
            return new Status(getLocalDocument(docID, null) != null ?
                    Status.CONFLICT: Status.NOT_FOUND);

        try {
            final KeyStore localDocs = new KeyStore(forest, "_local");
            return inTransaction(new Task() {
                @Override
                public Status run() {
                    KeyStoreWriter localWriter = forestTransaction.toKeyStoreWriter(localDocs);
                    try {
                        Document doc = localDocs.get(new Slice(docID.getBytes()));
                        if (!doc.exists()) {
                            return new Status(Status.NOT_FOUND);
                        } else if (!revID.equals(new String(doc.getMeta().getBuf()))) {
                            return new Status(Status.CONFLICT);
                        } else {
                            localWriter.del(doc);
                            return new Status(Status.OK);
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "Error in deleteLocalDocument()", e);
                        return new Status(Status.UNKNOWN);
                    } finally {
                        localWriter.delete();
                    }
                }
            });
        }catch(Exception e){
            Log.e(TAG, "Error in deleteLocalDocument()", e);
            return new Status(Status.UNKNOWN);
        }
    }

    /**
     * CBLDatabase+Insertion.m
     * static void convertRevIDs(NSArray* revIDs,
     *                          std::vector<revidBuffer> &historyBuffers,
     *                          std::vector<revid> &historyVector)
     */
    private static void convertRevIDs(List<String> history, VectorRevID historyVector){
        for(String revID : history){
            Log.w(TAG, "revID => " + revID);
            //RevID revid = new RevID(revID.getBytes());
            //historyVector.add(revid);
            //TODO add RevIDBuffer(String or byte[])
            RevIDBuffer revidbuffer = new RevIDBuffer(new Slice(revID.getBytes()));
            historyVector.add(revidbuffer);
        }
    }


    private interface Task{
        Status run();
    }

    private Status inTransaction(Task task) {
        Log.w(TAG, "inTransaction()");
        Status status = new Status(Status.OK);
        boolean commit = false;
        beginTransaction();
        try {
            status = task.run();
            commit = !status.isError();
        } finally {
            endTransaction(commit);
        }
        return status;
    }
}
