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
package com.couchbase.lite.replicator;

import com.couchbase.lite.ChangesOptions;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.DocumentChange;
import com.couchbase.lite.ReplicationFilter;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.CustomFuture;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.RevisionUtils;
import com.couchbase.lite.util.JSONUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;

import okhttp3.Response;

/**
 * @exclude
 */
@InterfaceAudience.Private
public class PusherInternal extends ReplicationInternal implements Database.ChangeListener {
    private static final String TAG = Log.TAG_SYNC;

    // Max in-memory size of buffered bulk_docs dictionary
    private static long kMaxBulkDocsObjectSize = 5 * 1000 * 1000;

    private boolean createTarget;
    private boolean creatingTarget;
    private boolean observing;
    private ReplicationFilter filter;
    private boolean dontSendMultipart = false;
    SortedSet<Long> pendingSequences;
    Long maxPendingSequence;
    final Object pendingSequencesLock = new Object();
    final Object changesLock = new Object();
    boolean doneBeginReplicating = false;
    List<RevisionInternal> queueChanges = new ArrayList<RevisionInternal>();
    private String str = null; // for toString()

    /**
     * Constructor
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public PusherInternal(Database db,
                          URL remote,
                          HttpClientFactory clientFactory,
                          Replication.Lifecycle lifecycle,
                          Replication parentReplication) {
        super(db, remote, clientFactory, lifecycle, parentReplication);
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

    @Override
    protected void stop() {
        if (stateMachine.isInState(ReplicationState.STOPPED))
            return;

        Log.d(Log.TAG_SYNC, "%s STOPPING...", toString());

        stopObserving();

        super.stop();

        // this has to be on a different thread than the replicator thread, or else it's a deadlock
        // because it might be waiting for jobs that have been scheduled, and not
        // yet executed (and which will never execute because this will block processing).
        String threadName = String.format(Locale.ENGLISH, "Thread-waitForPendingFutures[%s]", toString());
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // wait for all tasks completed
                    waitForPendingFutures();
                } catch (Exception e) {
                    Log.e(Log.TAG_SYNC, "stop.run() had exception: %s", e);
                } finally {
                    triggerStopImmediate();
                    Log.d(Log.TAG_SYNC, "PusherInternal stop.run() finished");
                }
            }
        }, threadName).start();
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

        Future future = sendAsyncRequest("PUT", "", null, new RemoteRequestCompletion() {

            @Override
            public void onCompletion(Response httpResponse, Object result, Throwable e) {
                creatingTarget = false;
                if (e != null && e instanceof RemoteRequestResponseException &&
                        ((RemoteRequestResponseException) e).getCode() != 412) {
                    Log.e(Log.TAG_SYNC, this + ": Failed to create remote db", e);
                    setError(e);
                    triggerStopGraceful();  // this is fatal: no db to push to!
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

        // reset doneBeginReplicating
        doneBeginReplicating = false;

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

        filter = compilePushReplicationFilter();
        if (filterName != null && filter == null) {
            Log.w(Log.TAG_SYNC, "%s: No ReplicationFilter registered for filter '%s'; ignoring",
                    this, filterName);
        }

        // Now listen for future changes (in continuous mode):
        if (isContinuous() && isRunning()) {
            observing = true;
            db.addChangeListener(this);
        }

        // Process existing changes since the last push:
        long lastSequenceLong = 0;
        if (lastSequence != null) {
            lastSequenceLong = Long.parseLong(lastSequence);
        }
        ChangesOptions options = new ChangesOptions();
        options.setIncludeConflicts(true);
        Log.d(Log.TAG_SYNC, "%s: Getting changes since %d", this, lastSequenceLong);
        final RevisionList changes = db.changesSince(lastSequenceLong, options, filter, filterParams);
        if (changes.size() > 0) {
            Log.d(Log.TAG_SYNC, "%s: Queuing %d changes since %d", this, changes.size(), lastSequenceLong);
            // NOTE: Needs to submit changes into inbox from RemoteRequest thread for beginReplication.
            //       RemoteRequest thread is observed by pendingFuture, if using other thread to
            //       submit changes  into inbox, there are chance both inbox and pendingFutures are
            //       empty.
            submitRevisions(changes);
        } else {
            Log.d(Log.TAG_SYNC, "%s: No changes since %d", this, lastSequenceLong);
        }

        // process queued changes by `changed()` callback
        synchronized (changesLock) {
            for (RevisionInternal rev : queueChanges) {
                if (!changes.contains(rev)) {
                    addToInbox(rev);
                }
            }
            doneBeginReplicating = true;
        }
    }

    /**
     * - (void) dbChanged: (NSNotification*)n in CBLRestPusher.m
     */
    @Override
    @InterfaceAudience.Private
    public void changed(Database.ChangeEvent event) {
        submitRevisions(event.getChanges());
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
        synchronized (pendingSequencesLock) {
            long seq = revisionInternal.getSequence();
            pendingSequences.add(seq);
            if (seq > maxPendingSequence) {
                maxPendingSequence = seq;
            }
        }
    }

    /**
     * Removes a revision from the "pending" set after it's been uploaded. Advances checkpoint.
     * - (void) removePending: (CBL_Revision*)rev in CBLRestPusher.m
     */
    @InterfaceAudience.Private
    private void removePending(RevisionInternal revisionInternal) {
        synchronized (pendingSequencesLock) {
            long seq = revisionInternal.getSequence();
            if (pendingSequences == null || pendingSequences.isEmpty()) {
                Log.w(Log.TAG_SYNC, "%s: removePending() called w/ rev: %s, but pendingSequences empty",
                        this, revisionInternal);
                if (revisionInternal.getBody() != null)
                    revisionInternal.getBody().release();
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
            if (revisionInternal.getBody() != null)
                revisionInternal.getBody().release();
        }
    }

    /**
     * - (void) processInbox: (CBL_RevisionList*)changes in CBLRestPusher.m
     */
    @Override
    @InterfaceAudience.Private
    protected void processInbox(final RevisionList changes) {

        Log.v(Log.TAG_SYNC, "processInbox() changes=" + changes.size());

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

        CustomFuture future = sendAsyncRequest("POST", "_revs_diff", diffs, new RemoteRequestCompletion() {

            @Override
            public void onCompletion(Response httpResponse, Object response, Throwable e) {

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
                                removePending(rev);
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

                            if (loadedRev.getPropertyForKey("_removed") != null &&
                                    ((Boolean) loadedRev.getPropertyForKey("_removed")).booleanValue()) {
                                // Filter out _removed revision:
                                removePending(rev);
                                continue;
                            }

                            RevisionInternal populatedRev = transformRevision(loadedRev);

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
    }

    /**
     * Post the revisions to the destination. "new_edits":false means that the server should
     * use the given _rev IDs instead of making up new ones.
     * <p/>
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

        CustomFuture future = sendAsyncRequest("POST", "_bulk_docs", bulkDocsBody, new RemoteRequestCompletion() {

            @Override
            public void onCompletion(Response httpResponse, Object result, Throwable e) {
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
        Map<String, Object> body = revision.getProperties();
        Map<String, Object> attachments = (Map<String, Object>) body.get("_attachments");

        for (String attachmentKey : attachments.keySet()) {
            Map<String, Object> attachment = (Map<String, Object>) attachments.get(attachmentKey);
            if (!attachment.containsKey("follows")) {
                return false;
            }
        }

        Log.d(Log.TAG_SYNC, "Uploading multipart request.  Revision: %s", revision);
        addToChangesCount(1);
        final String path = String.format(Locale.ENGLISH, "%s?new_edits=false", encodeDocumentId(revision.getDocID()));
        CustomFuture future = sendAsyncMultipartRequest("PUT", path, body, attachments,
                new RemoteRequestCompletion() {
                    @Override
                    public void onCompletion(Response httpResponse, Object result, Throwable e) {
                        try {
                            if (e != null) {
                                if (e instanceof RemoteRequestResponseException) {
                                    // Server doesn't like multipart, eh? Fall back to JSON.
                                    if (((RemoteRequestResponseException) e).getCode() == 415) {
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
            setError(new CouchbaseLiteException(Status.BAD_ATTACHMENT));
            return;
        }

        final String path = String.format(Locale.ENGLISH, "%s?new_edits=false", encodeDocumentId(rev.getDocID()));
        CustomFuture future = sendAsyncRequest("PUT",
                path,
                rev.getProperties(),
                new RemoteRequestCompletion() {
                    public void onCompletion(Response httpResponse, Object result, Throwable e) {
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
     * <p/>
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

    /**
     * Submit revisions into inbox for changes from changesSince()
     */
    private void submitRevisions(final RevisionList changes) {
        int remaining = changes.size();
        int size = batcher.getCapacity();
        int start = 0;
        while (remaining > 0) {
            if (size > remaining)
                size = remaining;
            RevisionList subChanges = new RevisionList(changes.subList(start, start + size));
            batcher.queueObjects(subChanges);
            start += size;
            remaining -= size;
            // if not running state anymore, exit from loop.
            if (!isRunning())
                break;
        }
    }

    /**
     * Submit revisions into inbox for changes from Database.ChangeListener.change(ChangeEvent)
     */
    private void submitRevisions(final List<DocumentChange> changes) {
        synchronized (changesLock) {
            try {
                java.net.URI remoteUri = remote.toURI();
                for (DocumentChange change : changes) {
                    // Skip revisions that originally came from the database I'm syncing to:
                    URL source = change.getSource();
                    if (source != null && source.toURI().equals(remoteUri))
                        return;
                    RevisionInternal rev = change.getAddedRevision();
                    if (rev == null)
                        continue;
                    if (getLocalDatabase().runFilter(filter, filterParams, rev)) {
                        if (doneBeginReplicating) {
                            // if not running state anymore, exit from loop.
                            if (!isRunning())
                                break;
                            RevisionInternal nuRev = rev.copyWithoutBody();
                            addToInbox(nuRev);
                        } else {
                            RevisionInternal nuRev = rev.copyWithoutBody();
                            queueChanges.add(nuRev);
                        }
                    }
                }
            } catch (java.net.URISyntaxException uriException) {
                // Not possible since it would not be an active replicator.
                // However, until we refactor everything to use java.net,
                // I'm not sure we have a choice but to swallow this.
                Log.e(Log.TAG_SYNC, "Active replicator found with invalid URI", uriException);
            }
        }
    }

    @Override
    protected void onBeforeScheduleRetry() {
        stopObserving();
    }

    @Override
    public String toString() {
        if (str == null) {
            String maskedRemote = "unknown";
            if (remote != null)
                maskedRemote = remote.toExternalForm();
            maskedRemote = maskedRemote.replaceAll("://.*:.*@", "://---:---@");
            String type = isPull() ? "pull" : "push";
            String replicationIdentifier = Utils.shortenString(remoteCheckpointDocID(), 5);
            if (replicationIdentifier == null)
                replicationIdentifier = "unknown";
            str = String.format(Locale.ENGLISH, "PusherInternal{%s, %s, %s}",
                    maskedRemote, type, replicationIdentifier);
        }
        return str;
    }
}
