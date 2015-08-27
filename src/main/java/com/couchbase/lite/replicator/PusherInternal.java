package com.couchbase.lite.replicator;

import com.couchbase.lite.BlobKey;
import com.couchbase.lite.BlobStore;
import com.couchbase.lite.ChangesOptions;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.DocumentChange;
import com.couchbase.lite.Manager;
import com.couchbase.lite.ReplicationFilter;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.CustomFuture;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.RemoteRequest;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.support.RevisionUtils;
import com.couchbase.lite.util.JSONUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;
import com.couchbase.org.apache.http.entity.mime.MultipartEntity;
import com.couchbase.org.apache.http.entity.mime.content.FileBody;
import com.couchbase.org.apache.http.entity.mime.content.StringBody;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @exclude
 */
@InterfaceAudience.Private
public class PusherInternal extends ReplicationInternal implements Database.ChangeListener {

    // Max in-memory size of buffered bulk_docs dictionary
    private static long kMaxBulkDocsObjectSize = 5*1000*1000;

    public static final int MAX_PENDING_DOCS = 200;

    private boolean createTarget;
    private boolean creatingTarget;
    private boolean observing;
    private ReplicationFilter filter;
    private boolean dontSendMultipart = false;
    SortedSet<Long> pendingSequences;
    Long maxPendingSequence;

    private boolean paused = false;
    private Object  pausedObj = new Object();

    /**
     * Constructor
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public PusherInternal(Database db, URL remote,
                          HttpClientFactory clientFactory,
                          ScheduledExecutorService workExecutor,
                          Replication.Lifecycle lifecycle,
                          Replication parentReplication) {
        super(db, remote, clientFactory, workExecutor, lifecycle, parentReplication);
    }

    @Override
    @InterfaceAudience.Public
    public boolean isPull() {
        return false;
    }

    @Override
    public boolean shouldCreateTarget() {
        return createTarget;
    }

    @Override
    public void setCreateTarget(boolean createTarget) {
        this.createTarget = createTarget;
    }

    protected void stopGraceful() {

        super.stopGraceful();

        Log.d(Log.TAG_SYNC, "PusherInternal stopGraceful()");

        // this has to be on a different thread than the replicator thread, or else it's a deadlock
        // because it might be waiting for jobs that have been scheduled, and not
        // yet executed (and which will never execute because this will block processing).
        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    Log.d(Log.TAG_SYNC, "PusherInternal stopGraceful()");

                    // wait for pending futures from the pusher (eg, oustanding http requests)
                    waitForPendingFutures();

                    stopObserving();
                } catch (Exception e) {
                    Log.e(Log.TAG_SYNC, "stopGraceful.run() had exception: %s", e);
                    e.printStackTrace();
                } finally {
                    triggerStopImmediate();
                    Log.d(Log.TAG_SYNC, "PusherInternal stopGraceful.run() finished");
                }
            }
        }).start();
    }

    protected boolean waitingForPendingFutures = false;
    protected Object lockWaitForPendingFutures = new Object();

    public void waitForPendingFutures() {
        if (waitingForPendingFutures) {
            return;
        }

        synchronized (lockWaitForPendingFutures) {
            waitingForPendingFutures = true;

            Log.d(Log.TAG_SYNC, "[waitForPendingFutures()] STARTED - thread id: " +
                    Thread.currentThread().getId());

            try {

                // wait for batcher's pending futures
                if (batcher != null) {
                    Log.d(Log.TAG_SYNC, "batcher.waitForPendingFutures()");
                    // TODO: should we call batcher.flushAll(); here?
                    batcher.waitForPendingFutures();
                    Log.d(Log.TAG_SYNC, "/batcher.waitForPendingFutures()");
                }

                while (!pendingFutures.isEmpty()) {
                    Future future = pendingFutures.take();
                    try {
                        Log.d(Log.TAG_SYNC, "calling future.get() on %s", future);
                        future.get();
                        Log.d(Log.TAG_SYNC, "done calling future.get() on %s", future);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }

                // since it's possible that in the process of waiting for pendingFutures,
                // new items were added to the batcher, let's wait for the batcher to
                // drain again.
                if (batcher != null) {
                    Log.d(Log.TAG_SYNC, "batcher.waitForPendingFutures()");
                    batcher.waitForPendingFutures();
                    Log.d(Log.TAG_SYNC, "/batcher.waitForPendingFutures()");
                }

                // If pendingFutures queue is empty and state is RUNNING, fireTrigger to IDLE
                // NOTE: in case of many documents sync, new Future tasks could be added into the queue.
                //       This is reason to check if queue is empty.
                if (pendingFutures.isEmpty()) {
                    Log.v(Log.TAG_SYNC, "[waitForPendingFutures()] state=" + stateMachine.getState());
                    Log.v(Log.TAG_SYNC, "[waitForPendingFutures()] fireTrigger(ReplicationTrigger.WAITING_FOR_CHANGES);");
                    fireTrigger(ReplicationTrigger.WAITING_FOR_CHANGES);
                }
            } catch (Exception e) {
                Log.e(Log.TAG_SYNC, "Exception waiting for pending futures: %s", e);
            } finally {
                Log.d(Log.TAG_SYNC, "[waitForPendingFutures()] END - thread id: " + Thread.currentThread().getId());
                waitingForPendingFutures = false;
            }
        }
    }

    /**
     * - (void) maybeCreateRemoteDB in CBL_Replicator.m
     */
    @Override
    @InterfaceAudience.Private
    protected void maybeCreateRemoteDB() {
        if (!createTarget) {
            return;
        }
        creatingTarget = true;
        Log.v(Log.TAG_SYNC, "Remote db might not exist; creating it...");

        Future future = sendAsyncRequest("PUT", "", null, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                creatingTarget = false;
                if (e != null && e instanceof HttpResponseException &&
                        ((HttpResponseException) e).getStatusCode() != 412) {
                    Log.e(Log.TAG_SYNC, this + ": Failed to create remote db", e);
                    setError(e);
                    triggerStop();  // this is fatal: no db to push to!
                } else {
                    Log.v(Log.TAG_SYNC, "%s: Created remote db", this);
                    createTarget = false;
                    beginReplicating();
                }
            }

        });
        pendingFutures.add(future);
    }

    /**
     * - (void) beginReplicating in CBL_Replicator.m
     */
    @Override
    @InterfaceAudience.Private
    public void beginReplicating() {
        // If we're still waiting to create the remote db, do nothing now. (This method will be
        // re-invoked after that request finishes; see -maybeCreateRemoteDB above.)

        Log.d(Log.TAG_SYNC, "%s: beginReplicating() called", this);

        // If we're still waiting to create the remote db, do nothing now. (This method will be
        // re-invoked after that request finishes; see maybeCreateRemoteDB() above.)
        if (creatingTarget) {
            Log.d(Log.TAG_SYNC, "%s: creatingTarget == true, doing nothing", this);
            return;
        }

        pendingSequences = Collections.synchronizedSortedSet(new TreeSet<Long>());
        try {
            maxPendingSequence = Long.parseLong(lastSequence);
        } catch (NumberFormatException e) {
            Log.w(Log.TAG_SYNC, "Error converting lastSequence: %s to long.  Using 0", lastSequence);
            maxPendingSequence = new Long(0);
        }

        if (filterName != null) {
            filter = db.getFilter(filterName);
        }
        if (filterName != null && filter == null) {
            Log.w(Log.TAG_SYNC, "%s: No ReplicationFilter registered for filter '%s'; ignoring",
                    this, filterName);
        }

        // Process existing changes since the last push:
        long lastSequenceLong = 0;
        if (lastSequence != null) {
            lastSequenceLong = Long.parseLong(lastSequence);
        }
        ChangesOptions options = new ChangesOptions();
        options.setIncludeConflicts(true);
        Log.d(Log.TAG_SYNC, "%s: Getting changes since %s", this, lastSequence);
        RevisionList changes = db.changesSince(lastSequenceLong, options, filter, filterParams);
        if (changes.size() > 0) {
            Log.d(Log.TAG_SYNC, "%s: Queuing %d changes since %s", this, changes.size(), lastSequence);
            int remaining = changes.size();
            int size = batcher.getCapacity();
            int start = 0;
            while(remaining > 0){
                if(size > remaining)
                    size = remaining;
                RevisionList subChanges = new RevisionList(changes.subList(start, start+size));
                batcher.queueObjects(subChanges);
                start += size;
                remaining -= size;
                pauseOrResume();
                waitIfPaused();
            }
        } else {
            Log.d(Log.TAG_SYNC, "%s: No changes since %s", this, lastSequence);
        }

        // Now listen for future changes (in continuous mode):
        if (isContinuous()) {
            observing = true;
            db.addChangeListener(this);
        } else {
            // if it's one shot, then we can stop graceful and wait for
            // pending work to drain.
            triggerStop();
        }
    }

    /**
     * - (void) stopObserving in CBL_Replicator.m
     */
    @InterfaceAudience.Private
    private void stopObserving() {
        if (observing) {
            observing = false;
            db.removeChangeListener(this);
        }
    }

    /**
     * - (BOOL) goOnline in CBL_Replicator.m
     */
    @Override
    protected void goOnline() {
        super.goOnline();

        Log.d(Log.TAG_SYNC, "%s: goOnline() called, calling checkSession()", this);
        // Note: checkSession() -> fetchRemoteCheckpointDoc() -> beginReplicating()
        //          => start observing database
        checkSession();
    }

    /**
     * - (BOOL) goOffline in CBL_Pusher.m
     */
    @Override
    protected void goOffline() {
        super.goOffline();
        stopObserving();
    }


    /**
     * Adds a local revision to the "pending" set that are awaiting upload:
     * - (void) addPending: (CBL_Revision*)rev in CBLRestPusher.m
     */
    @InterfaceAudience.Private
    private void addPending(RevisionInternal revisionInternal) {
        long seq = revisionInternal.getSequence();
        pendingSequences.add(seq);
        if (seq > maxPendingSequence) {
            maxPendingSequence = seq;
        }
    }

    /**
     * Removes a revision from the "pending" set after it's been uploaded. Advances checkpoint.
     * - (void) removePending: (CBL_Revision*)rev in CBLRestPusher.m
     */
    @InterfaceAudience.Private
    private void removePending(RevisionInternal revisionInternal) {
        long seq = revisionInternal.getSequence();
        if (pendingSequences == null || pendingSequences.isEmpty()) {
            Log.w(Log.TAG_SYNC, "%s: removePending() called w/ rev: %s, but pendingSequences empty",
                    this, revisionInternal);
            if(revisionInternal.getBody()!=null)
                revisionInternal.getBody().release();
            pauseOrResume();
            return;
        }
        boolean wasFirst = (seq == pendingSequences.first());
        if (!pendingSequences.contains(seq)) {
            Log.w(Log.TAG_SYNC, "%s: removePending: sequence %s not in set, for rev %s",
                    this, seq, revisionInternal);
        }
        pendingSequences.remove(seq);
        if (wasFirst) {
            // If I removed the first pending sequence, can advance the checkpoint:
            long maxCompleted;
            if (pendingSequences.size() == 0) {
                maxCompleted = maxPendingSequence;
            } else {
                maxCompleted = pendingSequences.first();
                --maxCompleted;
            }
            setLastSequence(Long.toString(maxCompleted));
        }
        if(revisionInternal.getBody()!=null)
            revisionInternal.getBody().release();
        pauseOrResume();
    }

    /**
     * - (void) dbChanged: (NSNotification*)n in CBLRestPusher.m
     */
    @Override
    @InterfaceAudience.Private
    public void changed(Database.ChangeEvent event) {
        List<DocumentChange> changes = event.getChanges();
        for (DocumentChange change : changes) {
            // Skip revisions that originally came from the database I'm syncing to:
            URL source = change.getSource();
            if (source != null && source.equals(remote)) {
                return;
            }
            RevisionInternal rev = change.getAddedRevision();
            if (getLocalDatabase().runFilter(filter, filterParams, rev)) {
                pauseOrResume();
                waitIfPaused();
                RevisionInternal nuRev = rev.copy();
                nuRev.setBody(null); //save memory
                addToInbox(nuRev);
            }
        }
    }

    /**
     * - (void) processInbox: (CBL_RevisionList*)changes in CBLRestPusher.m
     */
    @Override
    @InterfaceAudience.Private
    protected void processInbox(final RevisionList changes) {

        Log.v(Log.TAG_SYNC, "processInbox() changes="+changes.size());

        // Generate a set of doc/rev IDs in the JSON format that _revs_diff wants:
        // <http://wiki.apache.org/couchdb/HttpPostRevsDiff>
        Map<String, List<String>> diffs = new HashMap<String, List<String>>();
        for (RevisionInternal rev : changes) {
            String docID = rev.getDocID();
            List<String> revs = diffs.get(docID);
            if (revs == null) {
                revs = new ArrayList<String>();
                diffs.put(docID, revs);
            }
            revs.add(rev.getRevID());
            addPending(rev);
        }

        // Call _revs_diff on the target db:
        Log.v(Log.TAG_SYNC, "%s: posting to /_revs_diff", this);

        CustomFuture future = sendAsyncRequest("POST", "/_revs_diff", diffs, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(HttpResponse httpResponse, Object response, Throwable e) {

                Log.v(Log.TAG_SYNC, "%s: got /_revs_diff response", this);
                Map<String, Object> results = (Map<String, Object>) response;
                if (e != null) {
                    setError(e);
                } else {
                    if (results.size() != 0) {
                        // Go through the list of local changes again, selecting the ones the destination server
                        // said were missing and mapping them to a JSON dictionary in the form _bulk_docs wants:
                        List<Object> docsToSend = new ArrayList<Object>();
                        RevisionList revsToSend = new RevisionList();
                        long bufferedSize = 0;
                        for (RevisionInternal rev : changes) {
                            // Is this revision in the server's 'missing' list?
                            Map<String, Object> properties = null;
                            Map<String, Object> revResults = (Map<String, Object>) results.get(rev.getDocID());
                            if (revResults == null) {
                                continue;
                            }
                            List<String> revs = (List<String>) revResults.get("missing");
                            if (revs == null || !revs.contains(rev.getRevID())) {
                                removePending(rev);
                                continue;
                            }

                            // NOTE: force to load body by Database.loadRevisionBody()
                            // In SQLiteStore.loadRevisionBody() does not load data from database
                            // if sequence != 0 && body != null
                            rev.setSequence(0);
                            rev.setBody(null);

                            RevisionInternal loadedRev;
                            try {
                                loadedRev = db.loadRevisionBody(rev);
                            } catch (CouchbaseLiteException e1) {
                                Log.w(Log.TAG_SYNC, "%s Couldn't get local contents of %s", rev, PusherInternal.this);
                                continue;
                            }

                            RevisionInternal populatedRev = transformRevision(loadedRev);
                            loadedRev = null;

                            List<String> possibleAncestors = (List<String>) revResults.get("possible_ancestors");

                            properties = new HashMap<String, Object>(populatedRev.getProperties());
                            Map<String, Object> revisions = db.getRevisionHistoryDictStartingFromAnyAncestor(populatedRev, possibleAncestors);
                            properties.put("_revisions", revisions);
                            populatedRev.setProperties(properties);

                            // Strip any attachments already known to the target db:
                            if (properties.containsKey("_attachments")) {
                                // Look for the latest common ancestor and stub out older attachments:
                                int minRevPos = findCommonAncestor(populatedRev, possibleAncestors);

                                Status status = new Status(Status.OK);
                                if (!db.expandAttachments(populatedRev, minRevPos + 1, !dontSendMultipart, false, status)) {
                                    Log.w(Log.TAG_SYNC, "%s: Couldn't expand attachments of %s", this, populatedRev);
                                    continue;
                                }

                                properties = populatedRev.getProperties();
                                if (!dontSendMultipart && uploadMultipartRevision(populatedRev)) {
                                    continue;
                                }
                            }

                            if (properties == null || !properties.containsKey("_id")) {
                                throw new IllegalStateException("properties must contain a document _id");
                            }

                            revsToSend.add(rev);
                            docsToSend.add(properties);

                            bufferedSize += JSONUtils.estimate(properties);
                            if (bufferedSize > kMaxBulkDocsObjectSize) {
                                uploadBulkDocs(docsToSend, revsToSend);
                                docsToSend = new ArrayList<Object>();
                                revsToSend = new RevisionList();
                                bufferedSize = 0;
                            }
                        }

                        // Post the revisions to the destination:
                        uploadBulkDocs(docsToSend, revsToSend);

                    } else {
                        // None of the revisions are new to the remote
                        for (RevisionInternal revisionInternal : changes) {
                            removePending(revisionInternal);
                        }
                    }
                }
            }

        });
        future.setQueue(pendingFutures);
        pendingFutures.add(future);

        pauseOrResume();
    }

    /**
     * Post the revisions to the destination. "new_edits":false means that the server should
     * use the given _rev IDs instead of making up new ones.
     *
     * - (void) uploadBulkDocs: (NSArray*)docsToSend changes: (CBL_RevisionList*)changes
     * in CBLRestPusher.m
     */
    @InterfaceAudience.Private
    protected void uploadBulkDocs(List<Object> docsToSend, final RevisionList changes) {

        final int numDocsToSend = docsToSend.size();
        if (numDocsToSend == 0) {
            return;
        }

        Log.v(Log.TAG_SYNC, "%s: POSTing " + numDocsToSend + " revisions to _bulk_docs: %s", PusherInternal.this, docsToSend);
        addToChangesCount(numDocsToSend);

        Map<String, Object> bulkDocsBody = new HashMap<String, Object>();
        bulkDocsBody.put("docs", docsToSend);
        bulkDocsBody.put("new_edits", false);

        CustomFuture future = sendAsyncRequest("POST", "/_bulk_docs", bulkDocsBody, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                if (e == null) {
                    Set<String> failedIDs = new HashSet<String>();
                    // _bulk_docs response is really an array, not a dictionary!
                    List<Map<String, Object>> items = (List) result;
                    for (Map<String, Object> item : items) {
                        Status status = statusFromBulkDocsResponseItem(item);
                        if (status.isError()) {
                            // One of the docs failed to save.
                            Log.w(Log.TAG_SYNC, "%s: _bulk_docs got an error: %s", item, this);
                            // 403/Forbidden means validation failed; don't treat it as an error
                            // because I did my job in sending the revision. Other statuses are
                            // actual replication errors.
                            if (status.getCode() != Status.FORBIDDEN) {
                                String docID = (String) item.get("id");
                                failedIDs.add(docID);
                                // TODO - port from iOS
                                // NSURL* url = docID ? [_remote URLByAppendingPathComponent: docID] : nil;
                                // error = CBLStatusToNSError(status, url);
                            }
                        }
                    }

                    // Remove from the pending list all the revs that didn't fail:
                    for (RevisionInternal revisionInternal : changes) {
                        if (!failedIDs.contains(revisionInternal.getDocID())) {
                            removePending(revisionInternal);
                        }
                    }
                }
                if (e != null) {
                    setError(e);
                } else {
                    Log.v(Log.TAG_SYNC, "%s: POSTed to _bulk_docs", PusherInternal.this);
                }
                addToCompletedChangesCount(numDocsToSend);
            }
        });
        future.setQueue(pendingFutures);
        pendingFutures.add(future);
    }

    /**
     * in CBL_Pusher.m
     * - (CBLMultipartWriter*)multipartWriterForRevision: (CBL_Revision*)rev
     */
    @InterfaceAudience.Private
    private boolean uploadMultipartRevision(final RevisionInternal revision) {

        MultipartEntity multiPart = null;

        Map<String, Object> revProps = revision.getProperties();

        Map<String, Object> attachments = (Map<String, Object>) revProps.get("_attachments");
        for (String attachmentKey : attachments.keySet()) {
            Map<String, Object> attachment = (Map<String, Object>) attachments.get(attachmentKey);
            if (attachment.containsKey("follows")) {

                if (multiPart == null) {

                    multiPart = new MultipartEntity();

                    try {
                        String json = Manager.getObjectMapper().writeValueAsString(revProps);
                        Charset utf8charset = Charset.forName("UTF-8");
                        byte[] uncompressed = json.getBytes(utf8charset);
                        byte[] compressed = null;
                        byte[] data = uncompressed;
                        String contentEncoding = null;
                        if (uncompressed.length > RemoteRequest.MIN_JSON_LENGTH_TO_COMPRESS && canSendCompressedRequests()) {
                            compressed = Utils.compressByGzip(uncompressed);
                            if (compressed.length < uncompressed.length) {
                                data = compressed;
                                contentEncoding = "gzip";
                            }
                        }
                        // NOTE: StringBody.contentEncoding default value is null. Setting null value to contentEncoding does not cause any impact.
                        multiPart.addPart("param1", new StringBody(data, "application/json", utf8charset, contentEncoding));
                    } catch (IOException e) {
                        throw new IllegalArgumentException(e);
                    }
                }

                BlobStore blobStore = this.db.getAttachmentStore();
                String base64Digest = (String) attachment.get("digest");
                BlobKey blobKey = new BlobKey(base64Digest);
                String path = blobStore.getRawPathForKey(blobKey);
                File file = new File(path);
                if (!file.exists()) {
                    Log.w(Log.TAG_SYNC, "Unable to find blob file for blobKey: %s - Skipping upload of multipart revision.", blobKey);
                    return false;
                } else {
                    String contentType = null;
                    if (attachment.containsKey("content_type")) {
                        contentType = (String) attachment.get("content_type");
                    } else if (attachment.containsKey("type")) {
                        contentType = (String) attachment.get("type");
                    } else if (attachment.containsKey("content-type")) {
                        Log.w(Log.TAG_SYNC, "Found attachment that uses content-type" +
                                " field name instead of content_type (see couchbase-lite-android" +
                                " issue #80): %s", attachment);
                    }

                    // contentType = null causes Exception from FileBody of apache.
                    if(contentType == null)
                        contentType = "application/octet-stream"; // default

                    // NOTE: Content-Encoding might not be necessary to set. Apache FileBody does not set Content-Encoding.
                    //       FileBody always return null for getContentEncoding(), and Content-Encoding header is not set in multipart
                    // CBL iOS: https://github.com/couchbase/couchbase-lite-ios/blob/feb7ff5eda1e80bd00e5eb19f1d46c793f7a1951/Source/CBL_Pusher.m#L449-L452
                    String contentEncoding = null;
                    if (attachment.containsKey("encoding")) {
                        contentEncoding = (String) attachment.get("encoding");
                    }

                    FileBody fileBody = new CustomFileBody(file, attachmentKey, contentType, contentEncoding);
                    multiPart.addPart(attachmentKey, fileBody);
                }

            }
        }

        if (multiPart == null) {
            return false;
        }

        final String path = String.format("/%s?new_edits=false", encodeDocumentId(revision.getDocID()));

        Log.d(Log.TAG_SYNC, "Uploading multipart request.  Revision: %s", revision);

        addToChangesCount(1);

        CustomFuture future = sendAsyncMultipartRequest("PUT", path, multiPart, new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                try {
                    if (e != null) {
                        if (e instanceof HttpResponseException) {
                            // Server doesn't like multipart, eh? Fall back to JSON.
                            if (((HttpResponseException) e).getStatusCode() == 415) {
                                //status 415 = "bad_content_type"
                                dontSendMultipart = true;
                                uploadJsonRevision(revision);
                            }
                        } else {
                            Log.e(Log.TAG_SYNC, "Exception uploading multipart request", e);
                            setError(e);
                        }
                    } else {
                        Log.v(Log.TAG_SYNC, "Uploaded multipart request.  Revision: %s", revision);
                        removePending(revision);
                    }
                } finally {

                    addToCompletedChangesCount(1);

                }
            }
        });
        future.setQueue(pendingFutures);
        pendingFutures.add(future);

        return true;
    }

    /**
     * Fallback to upload a revision if uploadMultipartRevision failed due to the server's rejecting
     * multipart format.
     * - (void) uploadJSONRevision: (CBL_Revision*)originalRev in CBLRestPusher.m
     */
    private void uploadJsonRevision(final RevisionInternal rev) {
        // Get the revision's properties:
        if (!db.inlineFollowingAttachmentsIn(rev)) {
            error = new CouchbaseLiteException(Status.BAD_ATTACHMENT);
            return;
        }

        final String path = String.format("/%s?new_edits=false", encodeDocumentId(rev.getDocID()));
        CustomFuture future = sendAsyncRequest("PUT",
                path,
                rev.getProperties(),
                new RemoteRequestCompletionBlock() {
                    public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                        if (e != null) {
                            setError(e);
                        } else {
                            Log.v(Log.TAG_SYNC, "%s: Sent %s (JSON), response=%s", this, rev, result);
                            removePending(rev);
                        }
                    }
                });
        future.setQueue(pendingFutures);
        pendingFutures.add(future);
    }

    /**
     * Given a revision and an array of revIDs, finds the latest common ancestor revID
     * and returns its generation #. If there is none, returns 0.
     *
     * int CBLFindCommonAncestor(CBL_Revision* rev, NSArray* possibleRevIDs) in CBLRestPusher.m
     */
    private static int findCommonAncestor(RevisionInternal rev, List<String> possibleRevIDs) {
        if (possibleRevIDs == null || possibleRevIDs.size() == 0) {
            return 0;
        }
        List<String> history = Database.parseCouchDBRevisionHistory(rev.getProperties());

        //rev is missing _revisions property
        assert (history != null);

        boolean changed = history.retainAll(possibleRevIDs);
        String ancestorID = history.size() == 0 ? null : history.get(0);

        if (ancestorID == null) {
            return 0;
        }

        int generation = RevisionUtils.parseRevIDNumber(ancestorID);

        return generation;
    }

    // CustomFileBody to support contentEncoding. FileBody returns always null for getContentEncoding()
    private static class CustomFileBody extends FileBody {
        private String contentEncoding = null;

        public CustomFileBody(File file, String filename, String mimeType, String contentEncoding) {
            super(file, filename, mimeType, null);
            this.contentEncoding = contentEncoding;
        }

        @Override
        public String getContentEncoding() {
            return contentEncoding;
        }
    }


    private void pauseOrResume() {
        int pending = batcher.count() + pendingSequences.size();
        setPaused(pending >= MAX_PENDING_DOCS);
    }

    private void setPaused(boolean paused) {
        Log.v(Log.TAG, "setPaused: " + paused);
        synchronized (pausedObj) {
            if(this.paused != paused) {
                this.paused = paused;
                pausedObj.notifyAll();
            }
        }
    }

    private void waitIfPaused(){
        while (paused) {
            Log.v(Log.TAG, "Waiting: " + paused);
            synchronized (pausedObj) {
                try {
                    pausedObj.wait();
                } catch (InterruptedException e) {
                }
            }
        }
    }
}
