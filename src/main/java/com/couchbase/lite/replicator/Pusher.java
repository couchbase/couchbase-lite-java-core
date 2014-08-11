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
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.URIUtils;

import org.apache.http.client.HttpResponseException;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @exclude
 */
@InterfaceAudience.Private
public final class Pusher extends Replication implements Database.ChangeListener {

    private boolean createTarget;
    private boolean creatingTarget;
    private boolean observing;
    private ReplicationFilter filter;
    private boolean dontSendMultipart = false;
    SortedSet<Long> pendingSequences;
    Long maxPendingSequence;

    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ public Pusher(Database db, URL remote, boolean continuous, ScheduledExecutorService workExecutor) {
        this(db, remote, continuous, null, workExecutor);
    }

    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ public Pusher(Database db, URL remote, boolean continuous, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor) {
        super(db, remote, continuous, clientFactory, workExecutor);
        createTarget = false;
        observing = false;
    }

    @Override
    @InterfaceAudience.Public
    public boolean isPull() {
        return false;
    }

    @Override
    @InterfaceAudience.Public
    public boolean shouldCreateTarget() {
        return createTarget;
    }

    @Override
    @InterfaceAudience.Public
    public void setCreateTarget(boolean createTarget) {
        this.createTarget = createTarget;
    }


    @Override
    @InterfaceAudience.Public
    public void stop() {
        stopObserving();
        super.stop();
    }

    /**
     * Adds a local revision to the "pending" set that are awaiting upload:
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
     */
    @InterfaceAudience.Private
    private void removePending(RevisionInternal revisionInternal) {
        long seq = revisionInternal.getSequence();
        if (pendingSequences == null || pendingSequences.isEmpty()) {
            Log.w(Log.TAG_SYNC, "%s: removePending() called w/ rev: %s, but pendingSequences empty",
                    this, revisionInternal);
            return;
        }
        boolean wasFirst = (seq == pendingSequences.first());
        if (!pendingSequences.contains(seq)) {
            Log.w(Log.TAG_SYNC, "%s: removePending: sequence %s not in set, for rev %s", this, seq, revisionInternal);
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
    }

    @Override
    @InterfaceAudience.Private
    void maybeCreateRemoteDB() {
        if(!createTarget) {
            return;
        }
        creatingTarget = true;
        Log.v(Log.TAG_SYNC, "Remote db might not exist; creating it...");

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: maybeCreateRemoteDB() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncRequest("PUT", "", null, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable e) {
                try {
                    creatingTarget = false;
                    if(e != null && e instanceof HttpResponseException && ((HttpResponseException)e).getStatusCode() != 412) {
                        Log.e(Log.TAG_SYNC, this + ": Failed to create remote db", e);
                        setError(e);
                        stop();  // this is fatal: no db to push to!
                    } else {
                        Log.v(Log.TAG_SYNC, "%s: Created remote db", this);
                        createTarget = false;
                        beginReplicating();
                    }
                } finally {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: maybeCreateRemoteDB.sendAsyncRequest() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }
            }

        });
    }

    @Override
    @InterfaceAudience.Private
    public void beginReplicating() {

        Log.d(Log.TAG_SYNC, "%s: beginReplicating() called", this);

        // If we're still waiting to create the remote db, do nothing now. (This method will be
        // re-invoked after that request finishes; see maybeCreateRemoteDB() above.)
        if(creatingTarget) {
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

        if(filterName != null) {
            filter = db.getFilter(filterName);
        }
        if(filterName != null && filter == null) {
            Log.w(Log.TAG_SYNC, "%s: No ReplicationFilter registered for filter '%s'; ignoring", this, filterName);;
        }

        // Process existing changes since the last push:
        long lastSequenceLong = 0;
        if(lastSequence != null) {
            lastSequenceLong = Long.parseLong(lastSequence);
        }
        ChangesOptions options = new ChangesOptions();
        options.setIncludeConflicts(true);
        RevisionList changes = db.changesSince(lastSequenceLong, options, filter);
        if(changes.size() > 0) {
            batcher.queueObjects(changes);
            batcher.flush();
        }

        // Now listen for future changes (in continuous mode):
        if(continuous) {
            observing = true;
            db.addChangeListener(this);
        }

    }


    @InterfaceAudience.Private
    private void stopObserving() {
        if(observing) {
            observing = false;
            db.removeChangeListener(this);
        }
    }


    @Override
    @InterfaceAudience.Private
    public void changed(Database.ChangeEvent event) {
        List<DocumentChange> changes = event.getChanges();
        for (DocumentChange change : changes) {
            // Skip revisions that originally came from the database I'm syncing to:
            URL source = change.getSourceUrl();
            if(source != null && source.equals(remote)) {
                return;
            }
            RevisionInternal rev = change.getAddedRevision();
            Map<String, Object> paramsFixMe = null;  // TODO: these should not be null
            if (getLocalDatabase().runFilter(filter, paramsFixMe, rev)) {
                addToInbox(rev);
            }

        }

    }

    @Override
    @InterfaceAudience.Private
    protected void processInbox(final RevisionList changes) {

        // Generate a set of doc/rev IDs in the JSON format that _revs_diff wants:
        // <http://wiki.apache.org/couchdb/HttpPostRevsDiff>
        Map<String,List<String>> diffs = new HashMap<String,List<String>>();
        for (RevisionInternal rev : changes) {
            String docID = rev.getDocId();
            List<String> revs = diffs.get(docID);
            if(revs == null) {
                revs = new ArrayList<String>();
                diffs.put(docID, revs);
            }
            revs.add(rev.getRevId());
            addPending(rev);
        }

        // Call _revs_diff on the target db:
        Log.v(Log.TAG_SYNC, "%s: posting to /_revs_diff", this);

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: processInbox() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncRequest("POST", "/_revs_diff", diffs, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object response, Throwable e) {

                try {
                    Log.v(Log.TAG_SYNC, "%s: got /_revs_diff response", this);
                    Map<String, Object> results = (Map<String, Object>) response;
                    if (e != null) {
                        setError(e);
                        revisionFailed();
                    } else if (results.size() != 0) {
                        // Go through the list of local changes again, selecting the ones the destination server
                        // said were missing and mapping them to a JSON dictionary in the form _bulk_docs wants:
                        final List<Object> docsToSend = new ArrayList<Object>();
                        RevisionList revsToSend = new RevisionList();
                        for(RevisionInternal rev : changes) {
                            // Is this revision in the server's 'missing' list?
                            Map<String,Object> properties = null;
                            Map<String,Object> revResults = (Map<String,Object>)results.get(rev.getDocId());
                            if(revResults == null) {
                                continue;
                            }
                            List<String> revs = (List<String>)revResults.get("missing");
                            if(revs == null || !revs.contains(rev.getRevId())) {
                                removePending(rev);
                                continue;
                            }

                            // Get the revision's properties:
                            EnumSet<Database.TDContentOptions> contentOptions = EnumSet.of(
                                    Database.TDContentOptions.TDIncludeAttachments
                            );

                            if (!dontSendMultipart && revisionBodyTransformationBlock==null) {
                                contentOptions.add(Database.TDContentOptions.TDBigAttachmentsFollow);
                            }

                            RevisionInternal loadedRev;
                            try {
                                loadedRev = db.loadRevisionBody(rev, contentOptions);
                                properties = new HashMap<String,Object>(rev.getProperties());
                            } catch (CouchbaseLiteException e1) {
                                Log.w(Log.TAG_SYNC, "%s Couldn't get local contents of %s", rev, Pusher.this);
                                revisionFailed();
                                continue;
                            }

                            RevisionInternal populatedRev = transformRevision(loadedRev);

                            List<String> possibleAncestors = (List<String>)revResults.get("possible_ancestors");

                            properties = new HashMap<String,Object>(populatedRev.getProperties());
                            Map<String,Object> revisions = db.getRevisionHistoryDictStartingFromAnyAncestor(populatedRev, possibleAncestors);
                            properties.put("_revisions",revisions);
                            populatedRev.setProperties(properties);

                            // Strip any attachments already known to the target db:
                            if (properties.containsKey("_attachments")) {
                                // Look for the latest common ancestor and stub out older attachments:
                                int minRevPos = findCommonAncestor(populatedRev, possibleAncestors);

                                Database.stubOutAttachmentsInRevBeforeRevPos(populatedRev,minRevPos + 1,false);

                                properties = populatedRev.getProperties();

                                if (!dontSendMultipart && uploadMultipartRevision(populatedRev)) {
                                    continue;
                                }
                            }

                            if(properties == null || !properties.containsKey("_id")) {
                                throw new IllegalStateException("properties must contain a document _id");
                            }

                            revsToSend.add(rev);
                            docsToSend.add(properties);

                            //TODO: port this code from iOS
                                /*
                                bufferedSize += [CBLJSON estimateMemorySize: properties];
                                if (bufferedSize > kMaxBulkDocsObjectSize) {
                                    [self uploadBulkDocs: docsToSend changes: revsToSend];
                                    docsToSend = $marray();
                                    revsToSend = [[CBL_RevisionList alloc] init];
                                    bufferedSize = 0;
                                }
                                */

                        }

                        // Post the revisions to the destination:
                        uploadBulkDocs(docsToSend, revsToSend);

                    } else {
                        // None of the revisions are new to the remote
                        for (RevisionInternal revisionInternal : changes) {
                            removePending(revisionInternal);
                        }
                    }

                } finally {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: processInbox.sendAsyncRequest() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }
            }

        });

    }

    /**
     * Post the revisions to the destination. "new_edits":false means that the server should
     * use the given _rev IDs instead of making up new ones.
     */
    @InterfaceAudience.Private
    protected void uploadBulkDocs(List<Object> docsToSend, final RevisionList changes) {

        final int numDocsToSend = docsToSend.size();
        if (numDocsToSend == 0 ) {
            return;
        }

        Log.v(Log.TAG_SYNC, "%s: POSTing " + numDocsToSend + " revisions to _bulk_docs: %s", Pusher.this, docsToSend);
        addToChangesCount(numDocsToSend);

        Map<String,Object> bulkDocsBody = new HashMap<String,Object>();
        bulkDocsBody.put("docs", docsToSend);
        bulkDocsBody.put("new_edits", false);

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: uploadBulkDocs() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncRequest("POST", "/_bulk_docs", bulkDocsBody, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable e) {
                try {
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
                            if (!failedIDs.contains(revisionInternal.getDocId())) {
                                removePending(revisionInternal);
                            }
                        }

                    }
                    if (e != null) {
                        setError(e);
                        revisionFailed();
                    } else {
                        Log.v(Log.TAG_SYNC, "%s: POSTed to _bulk_docs", Pusher.this);
                    }
                    addToCompletedChangesCount(numDocsToSend);

                } finally {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: uploadBulkDocs.sendAsyncRequest() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }

            }
        });

    }

    @InterfaceAudience.Private
    private boolean uploadMultipartRevision(final RevisionInternal revision) {

        MultipartEntity multiPart = null;

        Map<String, Object> revProps = revision.getProperties();

        // TODO: refactor this to
        /*
            // Get the revision's properties:
            NSError* error;
            if (![_db inlineFollowingAttachmentsIn: rev error: &error]) {
                self.error = error;
                [self revisionFailed];
                return;
            }
         */
        Map<String, Object> attachments = (Map<String, Object>) revProps.get("_attachments");
        for (String attachmentKey : attachments.keySet()) {
            Map<String, Object> attachment = (Map<String, Object>) attachments.get(attachmentKey);
            if (attachment.containsKey("follows")) {

                if (multiPart == null) {

                    multiPart = new MultipartEntity();

                    try {
                        String json  = Manager.getObjectMapper().writeValueAsString(revProps);
                        Charset utf8charset = Charset.forName("UTF-8");
                        multiPart.addPart("param1", new StringBody(json, "application/json", utf8charset));

                    } catch (IOException e) {
                        throw new IllegalArgumentException(e);
                    }

                }

                BlobStore blobStore = this.db.getAttachments();
                String base64Digest = (String) attachment.get("digest");
                BlobKey blobKey = new BlobKey(base64Digest);
                String path = blobStore.pathForKey(blobKey);
                File file = new File(path);
                if (!file.exists()) {
                    Log.w(Log.TAG_SYNC, "Unable to find blob file for blobKey: %s - Skipping upload of multipart revision.", blobKey);
                    multiPart = null;
                }
                else {
                    String contentType = null;
                    if (attachment.containsKey("content_type")) {
                        contentType = (String) attachment.get("content_type");
                    }
                    else if (attachment.containsKey("content-type")) {
                        Log.w(Log.TAG_SYNC, "Found attachment that uses content-type" +
                                " field name instead of content_type (see couchbase-lite-android" +
                                " issue #80): %s", attachment);
                    }

                    FileBody fileBody = new FileBody(file, contentType);
                    multiPart.addPart(attachmentKey, fileBody);
                }

            }
        }

        if (multiPart == null) {
            return false;
        }

        String path = String.format("/%s?new_edits=false", revision.getDocId());

        Log.d(Log.TAG_SYNC, "Uploading multipart request.  Revision: %s", revision);

        addToChangesCount(1);

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: uploadMultipartRevision() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        sendAsyncMultipartRequest("PUT", path, multiPart, new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(Object result, Throwable e) {
                try {
                    if(e != null) {
                        if(e instanceof HttpResponseException) {
                            // Server doesn't like multipart, eh? Fall back to JSON.
                            if (((HttpResponseException) e).getStatusCode() == 415) {
                                //status 415 = "bad_content_type"
                                dontSendMultipart = true;
                                uploadJsonRevision(revision);
                            }
                        } else {
                            Log.e(Log.TAG_SYNC, "Exception uploading multipart request", e);
                            setError(e);
                            revisionFailed();
                        }
                    } else {
                        Log.v(Log.TAG_SYNC, "Uploaded multipart request.");
                        removePending(revision);
                    }
                } finally {

                    addToCompletedChangesCount(1);
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: uploadMultipartRevision() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }

            }
        });

        return true;

    }

    // Fallback to upload a revision if uploadMultipartRevision failed due to the server's rejecting
    // multipart format.
    private void uploadJsonRevision(final RevisionInternal rev) {
        // Get the revision's properties:
        if (!db.inlineFollowingAttachmentsIn(rev)) {
            error = new CouchbaseLiteException(Status.BAD_ATTACHMENT);
            revisionFailed();
            return;
        }

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: uploadJsonRevision() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        String path = String.format("/%s?new_edits=false", URIUtils.encode(rev.getDocId()));
        sendAsyncRequest("PUT",
        path,
        rev.getProperties(),
        new RemoteRequestCompletionBlock() {
            public void onCompletion(Object result, Throwable e) {
                if (e != null) {
                    setError(e);
                    revisionFailed();
                } else {
                    Log.v(Log.TAG_SYNC, "%s: Sent %s (JSON), response=%s", this, rev, result);
                    removePending(rev);
                }
                Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: uploadJsonRevision() calling asyncTaskFinished()", this, Thread.currentThread());

                asyncTaskFinished(1);
            }
        });
    }




    // Given a revision and an array of revIDs, finds the latest common ancestor revID
    // and returns its generation #. If there is none, returns 0.
    private static int findCommonAncestor(RevisionInternal rev, List<String> possibleRevIDs) {
        if (possibleRevIDs == null || possibleRevIDs.size() == 0) {
            return 0;
        }
        List<String> history = Database.parseCouchDBRevisionHistory(rev.getProperties());

        //rev is missing _revisions property
        assert(history != null);

        boolean changed = history.retainAll(possibleRevIDs);
        String ancestorID = history.size() == 0 ? null : history.get(0);

        if (ancestorID == null) {
            return 0;
        }

        int generation = Database.parseRevIDNumber(ancestorID);

        return generation;
    }
}

