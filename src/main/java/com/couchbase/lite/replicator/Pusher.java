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
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.Log;

import org.apache.http.client.HttpResponseException;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.entity.mime.content.StringBody;

import java.io.IOException;
import java.io.InputStream;
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
        boolean wasFirst = (seq == pendingSequences.first());
        if (!pendingSequences.contains(seq)) {
            Log.w(Database.TAG, this + " removePending: sequence " + seq + " not in set, for rev " + revisionInternal);
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
            lastSequence = Long.toString(maxCompleted);
        }
    }

    @Override
    @InterfaceAudience.Private
    void maybeCreateRemoteDB() {
        if(!createTarget) {
            return;
        }
        creatingTarget = true;
        Log.v(Database.TAG, "Remote db might not exist; creating it...");
        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": maybeCreateRemoteDB() calling asyncTaskStarted()");

        asyncTaskStarted();
        sendAsyncRequest("PUT", "", null, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable e) {
                try {
                    creatingTarget = false;
                    if(e != null && e instanceof HttpResponseException && ((HttpResponseException)e).getStatusCode() != 412) {
                        Log.e(Database.TAG, this + ": Failed to create remote db", e);
                        setError(e);
                        stop();  // this is fatal: no db to push to!
                    } else {
                        Log.v(Database.TAG, this + ": Created remote db");
                        createTarget = false;
                        beginReplicating();
                    }
                } finally {
                    Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": maybeCreateRemoteDB.onComplete() calling asyncTaskFinished()");
                    asyncTaskFinished(1);
                }
            }

        });
    }

    @Override
    @InterfaceAudience.Private
    public void beginReplicating() {

        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": beginReplicating() called");

        // If we're still waiting to create the remote db, do nothing now. (This method will be
        // re-invoked after that request finishes; see maybeCreateRemoteDB() above.)
        if(creatingTarget) {
            Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": creatingTarget == true, doing nothing");
            return;
        } else {
            Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": creatingTarget != true, continuing");
        }

        pendingSequences = Collections.synchronizedSortedSet(new TreeSet<Long>());
        try {
            maxPendingSequence = Long.parseLong(lastSequence);
        } catch (NumberFormatException e) {
            Log.w(Database.TAG, "Error converting lastSequence: " + lastSequence + " to long.  Using 0");
            maxPendingSequence = new Long(0);
        }

        if(filterName != null) {
            filter = db.getFilter(filterName);
        }
        if(filterName != null && filter == null) {
            Log.w(Database.TAG, String.format("%s: No ReplicationFilter registered for filter '%s'; ignoring", this, filterName));;
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
            try {
                observing = false;
                db.removeChangeListener(this);
            } finally {
                Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": stopObserving() calling asyncTaskFinished()");
                asyncTaskFinished(1);
            }

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
        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": processInbox() calling asyncTaskStarted()");
        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": posting to /_revs_diff: " + diffs);

        asyncTaskStarted();
        sendAsyncRequest("POST", "/_revs_diff", diffs, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object response, Throwable e) {

                try {

                    Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": /_revs_diff response: " + response);
                    Map<String, Object> results = (Map<String, Object>) response;
                    if (e != null) {
                        setError(e);
                        revisionFailed();
                    } else if (results.size() != 0) {
                        // Go through the list of local changes again, selecting the ones the destination server
                        // said were missing and mapping them to a JSON dictionary in the form _bulk_docs wants:
                        final List<Object> docsToSend = new ArrayList<Object>();
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
                                    Database.TDContentOptions.TDIncludeAttachments,
                                    Database.TDContentOptions.TDBigAttachmentsFollow
                            );

                            try {
                                db.loadRevisionBody(rev, contentOptions);
                            } catch (CouchbaseLiteException e1) {
                                String msg = String.format("%s Couldn't get local contents of %s", rev, Pusher.this);
                                Log.w(Database.TAG, msg);
                                revisionFailed();
                                continue;
                            }
                            properties = new HashMap<String,Object>(rev.getProperties());
                            assert properties.get("_revisions") != null;

                            // Strip any attachments already known to the target db:
                            if (properties.containsKey("_attachments")) {
                                /* TODO: port this ios code
                                 // Look for the latest common ancestor and stub out older attachments:
                                    NSArray* possible = revResults[@"possible_ancestors"];
                                    int minRevPos = findCommonAncestor(rev, possible);
                                    [CBLDatabase stubOutAttachmentsIn: rev beforeRevPos: minRevPos + 1
                                                   attachmentsFollow: NO];
                                    properties = rev.properties;
                                 */
                                if (uploadMultipartRevision(rev)) {
                                    continue;
                                }
                            }

                            if(properties != null) {

                                // TODO: ios code does not call db.getRevisionHistoryDict() here
                                // Add the _revisions list:
                                properties.put("_revisions", db.getRevisionHistoryDict(rev));

                                //now add it to the docs to send
                                docsToSend.add(properties);

                                assert properties.get("_id") != null;
                            }

                        }

                        // Post the revisions to the destination:
                        uploadBulkDocs(docsToSend, changes);

                    } else {
                        // None of the revisions are new to the remote
                        for (RevisionInternal revisionInternal : changes) {
                            removePending(revisionInternal);
                        }
                    }

                } finally {
                    Log.d(Database.TAG, Pusher.this + "|" + Thread.currentThread() + ": processInbox() calling asyncTaskFinished()");
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

        Log.v(Database.TAG, String.format("%s: POSTing " + numDocsToSend + " revisions to _bulk_docs: %s", Pusher.this, docsToSend));
        setChangesCount(getChangesCount() + numDocsToSend);

        Map<String,Object> bulkDocsBody = new HashMap<String,Object>();
        bulkDocsBody.put("docs", docsToSend);
        bulkDocsBody.put("new_edits", false);

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
                                Log.w(Database.TAG, this + " _bulk_docs got an error: " + item);
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
                        Log.v(Database.TAG, String.format("%s: POSTed to _bulk_docs: %s", Pusher.this, changes));
                    }
                    setCompletedChangesCount(getCompletedChangesCount() + numDocsToSend);

                } finally {
                    Log.d(Database.TAG, Pusher.this + "|" + Thread.currentThread() + ": processInbox-after_bulk_docs() calling asyncTaskFinished()");
                    asyncTaskFinished(1);
                }

            }
        });

    }

    @InterfaceAudience.Private
    private Status statusFromBulkDocsResponseItem(Map<String, Object> item) {

        try {
            if (!item.containsKey("error")) {
                return new Status(Status.OK);
            }
            String errorStr = (String) item.get("error");
            if (errorStr == null || errorStr.isEmpty()) {
                return new Status(Status.OK);
            }

            // 'status' property is nonstandard; TouchDB returns it, others don't.
            String statusString = (String) item.get("status");
            int status = Integer.parseInt(statusString);
            if (status >= 400) {
                return new Status(status);
            }
            // If no 'status' present, interpret magic hardcoded CouchDB error strings:
            if (errorStr.equalsIgnoreCase("unauthorized")) {
                return new Status(Status.UNAUTHORIZED);
            } else if (errorStr.equalsIgnoreCase("forbidden")) {
                return new Status(Status.FORBIDDEN);
            } else if (errorStr.equalsIgnoreCase("conflict")) {
                return new Status(Status.CONFLICT);
            } else {
                return new Status(Status.UPSTREAM_ERROR);
            }

        } catch (Exception e) {
            Log.e(Database.TAG, "Exception getting status from " + item, e);
        }
        return new Status(Status.OK);


    }

    /*
    @Override
    @InterfaceAudience.Private
    protected void processInboxOLD(final RevisionList inbox) {
        final long lastInboxSequence = inbox.get(inbox.size()-1).getSequence();
        // Generate a set of doc/rev IDs in the JSON format that _revs_diff wants:
        // <http://wiki.apache.org/couchdb/HttpPostRevsDiff>
        Map<String,List<String>> diffs = new HashMap<String,List<String>>();
        for (RevisionInternal rev : inbox) {
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
        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": processInbox() calling asyncTaskStarted()");
        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": posting to /_revs_diff: " + diffs);

        asyncTaskStarted();
        sendAsyncRequest("POST", "/_revs_diff", diffs, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object response, Throwable e) {

                try {
                    Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": /_revs_diff response: " + response);
                    Map<String,Object> results = (Map<String,Object>)response;
                    if(e != null) {
                        setError(e);
                        revisionFailed();
                    } else if(results.size() != 0) {
                        // Go through the list of local changes again, selecting the ones the destination server
                        // said were missing and mapping them to a JSON dictionary in the form _bulk_docs wants:
                        final List<Object> docsToSend = new ArrayList<Object>();
                        for(RevisionInternal rev : inbox) {
                            Map<String,Object> properties = null;
                            Map<String,Object> resultDoc = (Map<String,Object>)results.get(rev.getDocId());
                            if(resultDoc != null) {
                                List<String> revs = (List<String>)resultDoc.get("missing");
                                if(revs != null && revs.contains(rev.getRevId())) {
                                    //remote server needs this revision
                                    // Get the revision's properties
                                    if(rev.isDeleted()) {
                                        properties = new HashMap<String,Object>();
                                        properties.put("_id", rev.getDocId());
                                        properties.put("_rev", rev.getRevId());
                                        properties.put("_deleted", true);
                                    } else {
                                        // OPT: Shouldn't include all attachment bodies, just ones that have changed
                                        EnumSet<Database.TDContentOptions> contentOptions = EnumSet.of(
                                                Database.TDContentOptions.TDIncludeAttachments,
                                                Database.TDContentOptions.TDBigAttachmentsFollow
                                        );

                                        try {
                                            db.loadRevisionBody(rev, contentOptions);
                                        } catch (CouchbaseLiteException e1) {
                                            String msg = String.format("%s Couldn't get local contents of %s", rev, Pusher.this);
                                            Log.w(Database.TAG, msg);
                                            revisionFailed();
                                            continue;
                                        }
                                        properties = new HashMap<String,Object>(rev.getProperties());


                                    }
                                    if (properties.containsKey("_attachments")) {
                                        if (uploadMultipartRevision(rev)) {
                                            continue;
                                        }
                                    }
                                    if(properties != null) {
                                        // Add the _revisions list:
                                        properties.put("_revisions", db.getRevisionHistoryDict(rev));
                                        //now add it to the docs to send
                                        docsToSend.add(properties);
                                    }
                                }
                            }
                        }

                        // Post the revisions to the destination. "new_edits":false means that the server should
                        // use the given _rev IDs instead of making up new ones.
                        final int numDocsToSend = docsToSend.size();
                        if (numDocsToSend > 0 ) {
                            Map<String,Object> bulkDocsBody = new HashMap<String,Object>();
                            bulkDocsBody.put("docs", docsToSend);
                            bulkDocsBody.put("new_edits", false);
                            Log.v(Database.TAG, String.format("%s: POSTing " + numDocsToSend + " revisions to _bulk_docs: %s", Pusher.this, docsToSend));
                            setChangesCount(getChangesCount() + numDocsToSend);
                            Log.d(Database.TAG, Pusher.this + "|" + Thread.currentThread() + ": processInbox-before_bulk_docs() calling asyncTaskStarted()");

                            asyncTaskStarted();
                            sendAsyncRequest("POST", "/_bulk_docs", bulkDocsBody, new RemoteRequestCompletionBlock() {

                                @Override
                                public void onCompletion(Object result, Throwable e) {
                                    try {
                                        if(e != null) {
                                            setError(e);
                                            revisionFailed();
                                        } else {
                                            Log.v(Database.TAG, String.format("%s: POSTed to _bulk_docs: %s", Pusher.this, docsToSend));
                                            setLastSequence(String.format("%d", lastInboxSequence));
                                        }
                                        setCompletedChangesCount(getCompletedChangesCount() + numDocsToSend);
                                    } finally {
                                        Log.d(Database.TAG, Pusher.this + "|" + Thread.currentThread() + ": processInbox-after_bulk_docs() calling asyncTaskFinished()");
                                        asyncTaskFinished(1);
                                    }


                                }
                            });
                        }


                    } else {
                        // If none of the revisions are new to the remote, just bump the lastSequence:
                        setLastSequence(String.format("%d", lastInboxSequence));
                    }
                } finally {
                    Log.d(Database.TAG, Pusher.this + "|" + Thread.currentThread() + ": processInbox() calling asyncTaskFinished()");
                    asyncTaskFinished(1);
                }
            }

        });
    } */

    @InterfaceAudience.Private
    private boolean uploadMultipartRevision(final RevisionInternal revision) {

        MultipartEntity multiPart = null;

        Map<String, Object> revProps = revision.getProperties();
        revProps.put("_revisions", db.getRevisionHistoryDict(revision));

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
                InputStream inputStream = blobStore.blobStreamForKey(blobKey);
                if (inputStream == null) {
                    Log.w(Database.TAG, "Unable to find blob file for blobKey: " + blobKey + " - Skipping upload of multipart revision.");
                    multiPart = null;
                }
                else {
                    String contentType = null;
                    if (attachment.containsKey("content_type")) {
                        contentType = (String) attachment.get("content_type");
                    }
                    else if (attachment.containsKey("content-type")) {
                        String message = String.format("Found attachment that uses content-type" +
                                " field name instead of content_type (see couchbase-lite-android" +
                                " issue #80): " + attachment);
                        Log.w(Database.TAG, message);
                    }
                    multiPart.addPart(attachmentKey, new InputStreamBody(inputStream, contentType, attachmentKey));
                }

            }
        }

        if (multiPart == null) {
            return false;
        }

        String path = String.format("/%s?new_edits=false", revision.getDocId());

        // TODO: need to throttle these requests
        Log.d(Database.TAG, "Uploading multipart request.  Revision: " + revision);
        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": uploadMultipartRevision() calling asyncTaskStarted()");

        setChangesCount(getChangesCount() + 1);
        asyncTaskStarted();
        sendAsyncMultipartRequest("PUT", path, multiPart, new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(Object result, Throwable e) {
                try {
                    if(e != null) {
                        // TODO:
                        /*
                        if ($equal(error.domain, CBLHTTPErrorDomain)
                                    && error.code == kCBLStatusUnsupportedType) {
                              // Server doesn't like multipart, eh? Fall back to JSON.
                              _dontSendMultipart = YES;
                              [self uploadJSONRevision: rev];
                          }
                         */
                        Log.e(Database.TAG, "Exception uploading multipart request", e);
                        setError(e);
                        revisionFailed();
                    } else {
                        Log.d(Database.TAG, "Uploaded multipart request.  Result: " + result);
                        removePending(revision);
                    }
                } finally {
                    Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": uploadMultipartRevision() calling asyncTaskFinished()");
                    setCompletedChangesCount(getCompletedChangesCount() + 1);
                    asyncTaskFinished(1);

                }

            }
        });

        return true;

    }


}

