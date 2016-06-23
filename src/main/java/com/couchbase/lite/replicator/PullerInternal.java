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

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.Document;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Misc;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.TransactionalTask;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.support.BatchProcessor;
import com.couchbase.lite.support.Batcher;
import com.couchbase.lite.support.CustomFuture;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.SequenceMap;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.URIUtils;
import com.couchbase.lite.util.Utils;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import okhttp3.OkHttpClient;
import okhttp3.Response;

/**
 * Pull Replication
 *
 * @exclude
 */
@InterfaceAudience.Private
public class PullerInternal extends ReplicationInternal implements ChangeTrackerClient {

    private static final String TAG = Log.TAG_SYNC;

    private static final int MAX_OPEN_HTTP_CONNECTIONS = 16;

    // Maximum number of revs to fetch in a single bulk request
    public static final int MAX_REVS_TO_GET_IN_BULK = 50;

    // Maximum number of revision IDs to pass in an "?atts_since=" query param
    public static final int MAX_NUMBER_OF_ATTS_SINCE = 50;

    // Tune this parameter based on application needs.
    public static int CHANGE_TRACKER_RESTART_DELAY_MS = 10 * 1000;

    public static final int MAX_PENDING_DOCS = 200;

    private static final int INSERTION_BATCHER_DELAY = 250; // 0.25 Seconds

    private ChangeTracker changeTracker;
    protected SequenceMap pendingSequences;
    protected Boolean canBulkGet;  // Does the server support _bulk_get requests?
    protected List<RevisionInternal> revsToPull = Collections.synchronizedList(
            new ArrayList<RevisionInternal>(100));
    protected List<RevisionInternal> bulkRevsToPull = Collections.synchronizedList(
            new ArrayList<RevisionInternal>(100));
    protected List<RevisionInternal> deletedRevsToPull = Collections.synchronizedList(
            new ArrayList<RevisionInternal>(100));
    protected int httpConnectionCount;
    protected Batcher<RevisionInternal> downloadsToInsert;

    private String str = null;

    public PullerInternal(Database db,
                          URL remote,
                          HttpClientFactory clientFactory,
                          Replication.Lifecycle lifecycle,
                          Replication parentReplication) {
        super(db, remote, clientFactory, lifecycle, parentReplication);
    }

    /**
     * Actual work of starting the replication process.
     */
    protected void beginReplicating() {
        Log.v(TAG, "submit startReplicating()");
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if (isRunning()) {
                    Log.v(TAG, "start startReplicating()");
                    initPendingSequences();
                    initDownloadsToInsert();
                    startChangeTracker();
                }
                // start replicator ...
            }
        });
    }

    private void initDownloadsToInsert() {
        if (downloadsToInsert == null) {
            int capacity = 200;
            downloadsToInsert = new Batcher<RevisionInternal>(executor,
                    capacity, INSERTION_BATCHER_DELAY, new BatchProcessor<RevisionInternal>() {
                @Override
                public void process(List<RevisionInternal> inbox) {
                    insertDownloads(inbox);
                }
            });
        }
    }

    @Override
    protected void onBeforeScheduleRetry() {
        // stop change tracker
        if (changeTracker != null)
            changeTracker.stop();
    }

    public boolean isPull() {
        return true;
    }

    protected void maybeCreateRemoteDB() {
        // puller never needs to do this
    }

    protected void startChangeTracker() {
        // make sure not start new changeTracker if pull replicator is not running or idle
        if (!(stateMachine.isInState(ReplicationState.RUNNING) ||
                stateMachine.isInState(ReplicationState.IDLE)))
            return;

        // if changeTracker is already running, not start new one.
        if (changeTracker != null && changeTracker.isRunning())
            return;

        ChangeTracker.ChangeTrackerMode changeTrackerMode;

        // it always starts out as OneShot, but if its a continuous replication
        // it will switch to longpoll later.
        changeTrackerMode = ChangeTracker.ChangeTrackerMode.OneShot;

        Log.d(TAG, "%s: starting ChangeTracker with since=%s mode=%s",
                this, lastSequence, changeTrackerMode);
        changeTracker = new ChangeTracker(remote, changeTrackerMode, true, lastSequence, this);
        changeTracker.setAuthenticator(getAuthenticator());
        Log.d(TAG, "%s: started ChangeTracker %s", this, changeTracker);

        if (filterName != null) {
            changeTracker.setFilterName(filterName);
            if (filterParams != null) {
                changeTracker.setFilterParams(filterParams);
            }
        }
        changeTracker.setDocIDs(documentIDs);
        changeTracker.setRequestHeaders(requestHeaders);
        changeTracker.setContinuous(lifecycle == Replication.Lifecycle.CONTINUOUS);
        changeTracker.setActiveOnly(lastSequence == null && db.getDocumentCount() == 0);
        changeTracker.start();
    }

    /**
     * Process a bunch of remote revisions from the _changes feed at once
     */
    @Override
    @InterfaceAudience.Private
    protected void processInbox(RevisionList inbox) {
        Log.d(TAG, "processInbox called");

        if (db == null || !db.isOpen()) {
            Log.w(Log.TAG_SYNC, "%s: Database is null or closed. Unable to continue. db name is %s.", this, db.getName());
            return;
        }

        if (canBulkGet == null) {
            canBulkGet = serverIsSyncGatewayVersion("0.81");
        }

        // Ask the local database which of the revs are not known to it:
        String lastInboxSequence = ((PulledRevision) inbox.get(inbox.size() - 1)).getRemoteSequenceID();

        int numRevisionsRemoved = 0;
        try {
            // findMissingRevisions is the local equivalent of _revs_diff. it looks at the
            // array of revisions in "inbox" and removes the ones that already exist.
            // So whatever's left in 'inbox'
            // afterwards are the revisions that need to be downloaded.
            numRevisionsRemoved = db.findMissingRevisions(inbox);
        } catch (SQLException e) {
            Log.e(TAG, String.format(Locale.ENGLISH, "%s failed to look up local revs", this), e);
            inbox = null;
        }

        //introducing this to java version since inbox may now be null everywhere
        int inboxCount = 0;
        if (inbox != null) {
            inboxCount = inbox.size();
        }

        if (numRevisionsRemoved > 0) {
            Log.v(TAG, "%s: processInbox() setting changesCount to: %s",
                    this, getChangesCount().get() - numRevisionsRemoved);
            // May decrease the changesCount, to account for the revisions we just found out we don't need to get.
            addToChangesCount(-1 * numRevisionsRemoved);
        }

        if (inboxCount == 0) {
            // Nothing to do. Just bump the lastSequence.
            Log.d(TAG,
                    "%s no new remote revisions to fetch.  add lastInboxSequence (%s) to pendingSequences (%s)",
                    this, lastInboxSequence, pendingSequences);
            long seq = pendingSequences.addValue(lastInboxSequence);
            pendingSequences.removeSequence(seq);
            setLastSequence(pendingSequences.getCheckpointedValue());
            pauseOrResume();
            return;
        }

        Log.v(TAG, "%s: fetching %s remote revisions...", this, inboxCount);

        // Dump the revs into the queue of revs to pull from the remote db:
        for (int i = 0; i < inbox.size(); i++) {
            PulledRevision rev = (PulledRevision) inbox.get(i);
            if (canBulkGet || (rev.getGeneration() == 1 && !rev.isDeleted() && !rev.isConflicted())) {
                bulkRevsToPull.add(rev);
            } else {
                queueRemoteRevision(rev);
            }
            rev.setSequence(pendingSequences.addValue(rev.getRemoteSequenceID()));
        }
        pullRemoteRevisions();
        pauseOrResume();
    }

    /**
     * Start up some HTTP GETs, within our limit on the maximum simultaneous number
     * <p/>
     * The entire method is not synchronized, only the portion pulling work off the list
     * Important to not hold the synchronized block while we do network access
     */
    @InterfaceAudience.Private
    public void pullRemoteRevisions() {
        //find the work to be done in a synchronized block
        List<RevisionInternal> workToStartNow = new ArrayList<RevisionInternal>();
        List<RevisionInternal> bulkWorkToStartNow = new ArrayList<RevisionInternal>();

        synchronized (bulkRevsToPull) {
            while (httpConnectionCount + workToStartNow.size() < MAX_OPEN_HTTP_CONNECTIONS) {
                int nBulk = (bulkRevsToPull.size() < MAX_REVS_TO_GET_IN_BULK) ?
                        bulkRevsToPull.size() : MAX_REVS_TO_GET_IN_BULK;

                if (nBulk == 1) {
                    // Rather than pulling a single revision in 'bulk', just pull it normally:
                    queueRemoteRevision(bulkRevsToPull.remove(0));
                    nBulk = 0;
                }

                if (nBulk > 0) {
                    bulkWorkToStartNow.addAll(bulkRevsToPull.subList(0, nBulk));
                    bulkRevsToPull.subList(0, nBulk).clear();
                } else {
                    // Prefer to pull an existing revision over a deleted one:
                    if (revsToPull.size() == 0 && deletedRevsToPull.size() == 0) {
                        break;  // both queues are empty
                    } else if (revsToPull.size() > 0) {
                        workToStartNow.add(revsToPull.remove(0));
                    } else if (deletedRevsToPull.size() > 0) {
                        workToStartNow.add(deletedRevsToPull.remove(0));
                    }
                }
            }
        }

        //actually run it outside the synchronized block
        if (bulkWorkToStartNow.size() > 0) {
            pullBulkRevisions(bulkWorkToStartNow);
        }

        for (RevisionInternal work : workToStartNow) {
            pullRemoteRevision(work);
        }
    }

    // Get a bunch of revisions in one bulk request. Will use _bulk_get if possible.
    protected void pullBulkRevisions(List<RevisionInternal> bulkRevs) {

        int nRevs = bulkRevs.size();
        if (nRevs == 0) {
            return;
        }
        Log.d(TAG, "%s bulk-fetching %d remote revisions...", this, nRevs);
        Log.d(TAG, "%s bulk-fetching remote revisions: %s", this, bulkRevs);

        if (!canBulkGet) {
            pullBulkWithAllDocs(bulkRevs);
            return;
        }

        Log.v(TAG, "%s: POST _bulk_get", this);
        final List<RevisionInternal> remainingRevs = new ArrayList<RevisionInternal>(bulkRevs);

        ++httpConnectionCount;

        final RemoteBulkDownloaderRequest dl;
        try {
            dl = new RemoteBulkDownloaderRequest(
                    clientFactory,
                    remote,
                    true,
                    bulkRevs,
                    db,
                    this.requestHeaders,
                    new RemoteBulkDownloaderRequest.BulkDownloaderDocument() {
                        public void onDocument(Map<String, Object> props) {
                            // Got a revision!
                            // Find the matching revision in 'remainingRevs' and get its sequence:
                            RevisionInternal rev;
                            if (props.get("_id") != null) {
                                rev = new RevisionInternal(props);
                            } else {
                                rev = new RevisionInternal((String) props.get("id"),
                                        (String) props.get("rev"), false);
                            }

                            int pos = remainingRevs.indexOf(rev);
                            if (pos > -1) {
                                rev.setSequence(remainingRevs.get(pos).getSequence());
                                remainingRevs.remove(pos);
                            } else {
                                Log.w(TAG, "%s : Received unexpected rev rev", this);
                            }

                            if (props.get("_id") != null) {
                                // Add to batcher ... eventually it will be fed to -insertRevisions:.
                                queueDownloadedRevision(rev);
                            } else {
                                Status status = statusFromBulkDocsResponseItem(props);
                                Throwable err = new CouchbaseLiteException(status);
                                revisionFailed(rev, err);
                            }
                        }
                    },
                    new RemoteRequestCompletion() {

                        public void onCompletion(Response httpResponse, Object result, Throwable e) {
                            // The entire _bulk_get is finished:
                            if (e != null) {
                                setError(e);
                                completedChangesCount.addAndGet(remainingRevs.size());
                            }
                            --httpConnectionCount;
                            // Start another task if there are still revisions waiting to be pulled:
                            pullRemoteRevisions();
                        }
                    }
            );
        } catch (Exception e) {
            Log.e(TAG, "%s: pullBulkRevisions Exception: %s", this, e);
            return;
        }

        dl.setAuthenticator(getAuthenticator());
        // set compressed request - gzip
        dl.setCompressedRequest(canSendCompressedRequests());

        synchronized (remoteRequestExecutor) {
            if (!remoteRequestExecutor.isShutdown()) {
                Future future = remoteRequestExecutor.submit(dl);
                pendingFutures.add(future);
                runnables.put(future, dl);
            }
        }
    }

    private void putLocalDocument(final String docId, final Map<String, Object> localDoc) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    getLocalDatabase().putLocalDocument(docId, localDoc);
                } catch (CouchbaseLiteException e) {
                    Log.w(TAG, "Failed to store retryCount value for docId: " + docId, e);
                }
            }
        });
    }

    private void pruneFailedDownload(final String docId) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    getLocalDatabase().deleteLocalDocument(docId);
                } catch (CouchbaseLiteException e) {
                    Log.w(TAG, "Failed to delete local document: " + docId, e);
                }
            }
        });
    }

    // This invokes the tranformation block if one is installed and queues the resulting CBL_Revision
    private void queueDownloadedRevision(RevisionInternal rev) {

        if (revisionBodyTransformationBlock != null) {
            // Add 'file' properties to attachments pointing to their bodies:

            for (Map.Entry<String, Map<String, Object>> entry : (
                    (Map<String, Map<String, Object>>) rev.getProperties().get("_attachments")).entrySet()) {
                String name = entry.getKey();
                Map<String, Object> attachment = entry.getValue();
                attachment.remove("file");
                if (attachment.get("follows") != null && attachment.get("data") == null) {
                    String filePath = db.fileForAttachmentDict(attachment).getPath();
                    if (filePath != null)
                        attachment.put("file", filePath);
                }
            }

            RevisionInternal xformed = transformRevision(rev);
            if (xformed == null) {
                Log.v(TAG, "%s: Transformer rejected revision %s", this, rev);
                pendingSequences.removeSequence(rev.getSequence());
                lastSequence = pendingSequences.getCheckpointedValue();
                pauseOrResume();
                return;
            }
            rev = xformed;

            // Clean up afterwards
            Map<String, Map<String, Object>> attachments = (Map<String, Map<String, Object>>) rev.getProperties().get("_attachments");

            for (Map.Entry<String, Map<String, Object>> entry : attachments.entrySet()) {
                Map<String, Object> attachment = entry.getValue();
                attachment.remove("file");
            }
        }

        if (rev != null && rev.getBody() != null)
            rev.getBody().compact();

        downloadsToInsert.queueObject(rev);
    }


    // Get as many revisions as possible in one _all_docs request.
    // This is compatible with CouchDB, but it only works for revs of generation 1 without attachments.

    protected void pullBulkWithAllDocs(final List<RevisionInternal> bulkRevs) {
        // http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API

        ++httpConnectionCount;
        final RevisionList remainingRevs = new RevisionList(bulkRevs);

        Collection<String> keys = CollectionUtils.transform(bulkRevs,
                new CollectionUtils.Functor<RevisionInternal, String>() {
                    public String invoke(RevisionInternal rev) {
                        return rev.getDocID();
                    }
                }
        );

        Map<String, Object> body = new HashMap<String, Object>();
        body.put("keys", keys);

        Future future = sendAsyncRequest("POST",
                "_all_docs?include_docs=true",
                body,
                new RemoteRequestCompletion() {

                    public void onCompletion(Response httpResponse, Object result, Throwable e) {

                        Map<String, Object> res = (Map<String, Object>) result;

                        if (e != null) {
                            setError(e);

                            // TODO: There is a known bug caused by the line below, which is
                            // TODO: causing testMockSinglePullCouchDb to fail when running on a Nexus5 device.
                            // TODO: (the batching behavior is different in that case)
                            // TODO: See https://github.com/couchbase/couchbase-lite-java-core/issues/271
                            // completedChangesCount.addAndGet(bulkRevs.size());

                        } else {
                            // Process the resulting rows' documents.
                            // We only add a document if it doesn't have attachments, and if its
                            // revID matches the one we asked for.
                            List<Map<String, Object>> rows = (List<Map<String, Object>>) res.get("rows");
                            Log.v(TAG, "%s checking %d bulk-fetched remote revisions", this, rows.size());
                            for (Map<String, Object> row : rows) {
                                Map<String, Object> doc = (Map<String, Object>) row.get("doc");
                                if (doc != null && doc.get("_attachments") == null) {
                                    RevisionInternal rev = new RevisionInternal(doc);
                                    RevisionInternal removedRev = remainingRevs.removeAndReturnRev(rev);
                                    if (removedRev != null) {
                                        rev.setSequence(removedRev.getSequence());
                                        queueDownloadedRevision(rev);
                                    }
                                } else {
                                    Status status = statusFromBulkDocsResponseItem(row);
                                    if (status.isError() && row.containsKey("key") && row.get("key") != null) {
                                        RevisionInternal rev = remainingRevs.revWithDocId((String) row.get("key"));
                                        if (rev != null) {
                                            remainingRevs.remove(rev);
                                            revisionFailed(rev, new CouchbaseLiteException(status));
                                        }
                                    }

                                }
                            }
                        }

                        // Any leftover revisions that didn't get matched will be fetched individually:
                        if (remainingRevs.size() > 0) {
                            Log.v(TAG,
                                    "%s bulk-fetch didn't work for %d of %d revs; getting individually",
                                    this, remainingRevs.size(), bulkRevs.size());
                            for (RevisionInternal rev : remainingRevs) {
                                queueRemoteRevision(rev);
                            }
                            pullRemoteRevisions();
                        }

                        --httpConnectionCount;
                        // Start another task if there are still revisions waiting to be pulled:
                        pullRemoteRevisions();
                    }
                });
        pendingFutures.add(future);
    }

    /**
     * This will be called when _revsToInsert fills up:
     */
    @InterfaceAudience.Private
    public void insertDownloads(final List<RevisionInternal> downloads) {

        Log.d(TAG, this + " inserting " + downloads.size() + " revisions...");
        final long time = System.currentTimeMillis();
        Collections.sort(downloads, getRevisionListComparator());

        db.getStore().runInTransaction(new TransactionalTask() {
            @Override
            public boolean run() {
                boolean success = false;
                try {
                    for (RevisionInternal rev : downloads) {
                        long fakeSequence = rev.getSequence();
                        List<String> history = db.parseCouchDBRevisionHistory(rev.getProperties());
                        if (history.isEmpty() && rev.getGeneration() > 1) {
                            Log.w(TAG, "%s: Missing revision history in response for: %s", this, rev);
                            setError(new CouchbaseLiteException(Status.UPSTREAM_ERROR));
                            continue;
                        }

                        Log.v(TAG, "%s: inserting %s %s", this, rev.getDocID(), history);

                        // Insert the revision
                        try {
                            db.forceInsert(rev, history, remote);
                        } catch (CouchbaseLiteException e) {
                            if (e.getCBLStatus().getCode() == Status.FORBIDDEN) {
                                Log.i(TAG, "%s: Remote rev failed validation: %s", this, rev);
                            } else {
                                Log.w(TAG, "%s: failed to write %s: status=%s",
                                        this, rev, e.getCBLStatus().getCode());
                                setError(new RemoteRequestResponseException(e.getCBLStatus().getCode(), null));
                                continue;
                            }
                        }

                        //if(rev.getBody() != null) rev.getBody().release();
                        if (rev.getBody() != null) rev.getBody().compact();

                        // Mark this revision's fake sequence as processed:
                        pendingSequences.removeSequence(fakeSequence);
                    }

                    Log.v(TAG, "%s: finished inserting %d revisions", this, downloads.size());
                    success = true;

                } catch (SQLException e) {
                    Log.e(TAG, this + ": Exception inserting revisions", e);
                } finally {
                    if (success) {

                        // Checkpoint:
                        setLastSequence(pendingSequences.getCheckpointedValue());

                        long delta = System.currentTimeMillis() - time;
                        Log.v(TAG,
                                "%s: inserted %d revs in %d milliseconds",
                                this, downloads.size(), delta);

                        int newCompletedChangesCount = getCompletedChangesCount().get() + downloads.size();
                        Log.d(TAG,
                                "%s insertDownloads() updating completedChangesCount from %d -> %d ",
                                this, getCompletedChangesCount().get(), newCompletedChangesCount);

                        addToCompletedChangesCount(downloads.size());
                    }

                    pauseOrResume();
                    return success;
                }
            }
        });
    }

    @InterfaceAudience.Private
    private static Comparator<RevisionInternal> getRevisionListComparator() {
        return new Comparator<RevisionInternal>() {

            public int compare(RevisionInternal reva, RevisionInternal revb) {
                return Misc.SequenceCompare(reva.getSequence(), revb.getSequence());
            }

        };
    }

    private void revisionFailed(RevisionInternal rev, Throwable throwable) {
        if (!Utils.isTransientError(throwable)) {
            Log.v(TAG, "%s: giving up on %s: %s", this, rev, throwable);
            pendingSequences.removeSequence(rev.getSequence());
            pauseOrResume();
        }
        completedChangesCount.getAndIncrement();
    }

    /**
     * Fetches the contents of a revision from the remote db, including its parent revision ID.
     * The contents are stored into rev.properties.
     */
    @InterfaceAudience.Private
    public void pullRemoteRevision(final RevisionInternal rev) {

        Log.d(TAG, "%s: pullRemoteRevision with rev: %s", this, rev);

        ++httpConnectionCount;

        // Construct a query. We want the revision history, and the bodies of attachments that have
        // been added since the latest revisions we have locally.
        // See: http://wiki.apache.org/couchdb/HTTP_Document_API#Getting_Attachments_With_a_Document
        StringBuilder path = new StringBuilder(encodeDocumentId(rev.getDocID()));
        path.append("?rev=").append(URIUtils.encode(rev.getRevID()));
        path.append("&revs=true&attachments=true");

        // If the document has attachments, add an 'atts_since' param with a list of
        // already-known revisions, so the server can skip sending the bodies of any
        // attachments we already have locally:
        AtomicBoolean hasAttachment = new AtomicBoolean(false);
        List<String> knownRevs = db.getPossibleAncestorRevisionIDs(rev,
                PullerInternal.MAX_NUMBER_OF_ATTS_SINCE, hasAttachment);
        if (hasAttachment.get() && knownRevs != null && knownRevs.size() > 0) {
            path.append("&atts_since=");
            path.append(joinQuotedEscaped(knownRevs));
        }

        //create a final version of this variable for the log statement inside
        //FIXME find a way to avoid this
        final String pathInside = path.toString();
        CustomFuture future = sendAsyncMultipartDownloaderRequest("GET", pathInside,
                null, db, new RemoteRequestCompletion() {

                    @Override
                    public void onCompletion(Response httpResponse, Object result, Throwable e) {
                        if (e != null) {
                            Log.w(TAG, "Error pulling remote revision: %s", e, this);
                            if (Utils.isDocumentError(e)) {
                                // Revision is missing or not accessible:
                                revisionFailed(rev, e);
                            } else {
                                // Request failed:
                                setError(e);
                            }
                        } else {
                            Map<String, Object> properties = (Map<String, Object>) result;
                            PulledRevision gotRev = new PulledRevision(properties);
                            gotRev.setSequence(rev.getSequence());

                            Log.d(TAG, "%s: pullRemoteRevision add rev: %s to batcher: %s",
                                    PullerInternal.this, gotRev, downloadsToInsert);

                            if (gotRev.getBody() != null)
                                gotRev.getBody().compact();

                            // Add to batcher ... eventually it will be fed to -insertRevisions:.
                            downloadsToInsert.queueObject(gotRev);
                        }

                        // Note that we've finished this task:
                        --httpConnectionCount;

                        // Start another task if there are still revisions waiting to be pulled:
                        pullRemoteRevisions();
                    }
                });
        future.setQueue(pendingFutures);
        pendingFutures.add(future);
    }

    @InterfaceAudience.Private
    public static String joinQuotedEscaped(List<String> strings) {
        if (strings.size() == 0) {
            return "[]";
        }
        byte[] json = null;
        try {
            json = Manager.getObjectMapper().writeValueAsBytes(strings);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to serialize json", e);
        }
        return URIUtils.encode(new String(json));
    }

    /**
     * Add a revision to the appropriate queue of revs to individually GET
     */
    @InterfaceAudience.Private
    protected void queueRemoteRevision(RevisionInternal rev) {
        if (rev.isDeleted()) {
            deletedRevsToPull.add(rev);
        } else {
            revsToPull.add(rev);
        }
    }

    private void initPendingSequences() {

        if (pendingSequences == null) {
            pendingSequences = new SequenceMap();
            if (getLastSequence() != null) {
                // Prime _pendingSequences so its checkpointedValue will reflect the last known seq:
                long seq = pendingSequences.addValue(getLastSequence());
                pendingSequences.removeSequence(seq);
                assert (pendingSequences.getCheckpointedValue().equals(getLastSequence()));
            }
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getLastSequence() {
        return lastSequence;
    }

    ////////////////////////////////////////////////////////////
    // Implementations of CancellableRunnable
    ////////////////////////////////////////////////////////////

    @Override
    public OkHttpClient getOkHttpClient() {
        return clientFactory.getOkHttpClient();
    }

    @Override
    public void changeTrackerReceivedChange(final Map<String, Object> change) {
        try {
            Log.d(TAG, "changeTrackerReceivedChange: %s", change);
            processChangeTrackerChange(change);
        } catch (Exception e) {
            Log.e(TAG, "Error processChangeTrackerChange(): %s", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void changeTrackerStopped(ChangeTracker tracker) {
        // this callback will be on the changetracker thread, but we need
        // to do the work on the replicator thread.
        synchronized (executor) {
            if (!executor.isShutdown()) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            processChangeTrackerStopped(changeTracker);
                        } catch (RuntimeException e) {
                            Log.e(Log.TAG_CHANGE_TRACKER, "Unknown Error in processChangeTrackerStopped()", e);
                            throw e;
                        }
                    }
                });
            }
        }
    }

    @Override
    public void changeTrackerFinished(ChangeTracker tracker) {
        Log.d(TAG, "changeTrackerFinished");
    }

    @Override
    public void changeTrackerCaughtUp() {
        Log.d(TAG, "changeTrackerCaughtUp");
        // this has to be on a different thread than the replicator thread, or else it's a deadlock
        // because it might be waiting for jobs that have been scheduled, and not
        // yet executed (and which will never execute because this will block processing).
        waitForPendingFuturesWithNewThread();
    }

    ////////////////////////////////////////////////////////////
    // Protected or Private Methods
    ////////////////////////////////////////////////////////////


    /**
     * in CBL_Puller.m
     * - (void) changeTrackerReceivedSequence: (id)remoteSequenceID
     * docID: (NSString*)docID
     * revIDs: (NSArray*)revIDs
     * deleted: (BOOL)deleted
     */
    protected void processChangeTrackerChange(final Map<String, Object> change) {
        // Process each change from the feed:
        String docID = (String) change.get("id");
        if (docID == null || !Document.isValidDocumentId(docID))
            return;

        String lastSequence = change.get("seq").toString();
        boolean deleted = (change.containsKey("deleted") &&
                change.get("deleted").equals(Boolean.TRUE));
        List<Map<String, Object>> changes = (List<Map<String, Object>>) change.get("changes");
        for (Map<String, Object> changeDict : changes) {
            String revID = (String) changeDict.get("rev");
            if (revID == null) {
                continue;
            }

            PulledRevision rev = new PulledRevision(docID, revID, deleted);

            // Remember its remote sequence ID (opaque), and make up a numeric sequence
            // based on the order in which it appeared in the _changes feed:
            rev.setRemoteSequenceID(lastSequence);

            if (changes.size() > 1)
                rev.setConflicted(true);

            Log.d(TAG, "%s: adding rev to inbox %s", this, rev);

            Log.v(TAG,
                    "%s: changeTrackerReceivedChange() incrementing changesCount by 1", this);

            // this is purposefully done slightly different than the ios version
            addToChangesCount(1);

            addToInbox(rev);
        }

        pauseOrResume();
    }


    private void processChangeTrackerStopped(ChangeTracker tracker) {
        Log.d(TAG, "changeTrackerStopped.  lifecycle: %s", lifecycle);
        switch (lifecycle) {
            case ONESHOT:
                if (tracker.getLastError() != null) {
                    setError(tracker.getLastError());
                }
                // once replication finished, needs to stop replicator gracefully.
                waitForPendingFuturesWithNewThread();
                break;
            case CONTINUOUS:
                if (stateMachine.isInState(ReplicationState.OFFLINE)) {
                    // in this case, we don't want to do anything here, since
                    // we told the change tracker to go offline ..
                    Log.d(TAG, "Change tracker stopped because we are going offline");
                } else if (stateMachine.isInState(ReplicationState.STOPPING) ||
                        stateMachine.isInState(ReplicationState.STOPPED)) {
                    Log.d(TAG, "Change tracker stopped because replicator is stopping or stopped.");
                } else {
                    // otherwise, try to restart the change tracker, since it should
                    // always be running in continuous replications
                    String msg = "Change tracker stopped during continuous replication";
                    Log.w(TAG, msg);
                    parentReplication.setLastError(new Exception(msg));
                    fireTrigger(ReplicationTrigger.WAITING_FOR_CHANGES);
                    Log.d(TAG, "Scheduling change tracker restart in %d ms", CHANGE_TRACKER_RESTART_DELAY_MS);
                    executor.schedule(new Runnable() {
                        @Override
                        public void run() {
                            // the replication may have been stopped by the time this scheduled fires
                            // so we need to check the state here.
                            if (stateMachine.isInState(ReplicationState.RUNNING) ||
                                    stateMachine.isInState(ReplicationState.IDLE)) {
                                Log.d(TAG, "%s still running, restarting change tracker", this);
                                startChangeTracker();
                            } else {
                                Log.d(TAG, "%s still no longer running, not restarting change tracker", this);
                            }
                        }
                    }, CHANGE_TRACKER_RESTART_DELAY_MS, TimeUnit.MILLISECONDS);
                }
                break;
            default:
                Log.e(TAG, "Unknown lifecycle: %s", lifecycle);
        }
    }


    private void waitForPendingFuturesWithNewThread() {
        String threadName = String.format(Locale.ENGLISH, "Thread-waitForPendingFutures[%s]", toString());
        new Thread(new Runnable() {
            @Override
            public void run() {
                waitForPendingFutures();
            }
        }, threadName).start();
    }


    /**
     * Implementation of BlockingQueueListener.changed(EventType, Object, BlockingQueue) for Pull Replication
     * <p/>
     * Note: Pull replication needs to send IDLE after PUT /{db}/_local.
     * However sending IDLE from Push replicator breaks few unit test cases.
     * This is reason changed() method was override for pull replicatoin
     */
    @Override
    public void changed(EventType type, Object o, BlockingQueue queue) {
        if ((type == EventType.PUT || type == EventType.ADD) &&
                isContinuous() &&
                !queue.isEmpty()) {

            synchronized (lockWaitForPendingFutures) {
                if (waitingForPendingFutures) {
                    return;
                }
            }

            fireTrigger(ReplicationTrigger.RESUME);
            waitForPendingFuturesWithNewThread();
        }
    }

    @Override
    protected void stop() {
        if (stateMachine.isInState(ReplicationState.STOPPED))
            return;

        Log.d(TAG, "%s STOPPING...", toString());

        // stop change tracker
        if (changeTracker != null)
            changeTracker.stop();

        // clear downloadsToInsert batcher.
        if (downloadsToInsert != null) {
            // Not waiting here to avoid deadlock as this method is executed
            // inside the same WorkExecutor as the downloadsToInsert batcher.
            // All scheduled objects will be waited to completed by calling
            // waitForAllTasksCompleted() below.
            downloadsToInsert.flushAll(false);
        }

        super.stop();

        // this has to be on a different thread than the replicator thread, or else it's a deadlock
        // because it might be waiting for jobs that have been scheduled, and not
        // yet executed (and which will never execute because this will block processing).
        String threadName = String.format(Locale.ENGLISH, "Thread.waitForAllTasksCompleted[%s]", toString());
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // wait for all tasks completed
                    waitForAllTasksCompleted();
                } catch (Exception e) {
                    Log.e(TAG, "stop.run() had exception: %s", e);
                } finally {
                    // stop replicator immediate
                    triggerStopImmediate();
                    Log.d(TAG, "PullerInternal stop.run() finished");
                }
            }
        }, threadName).start();
    }

    protected void waitForAllTasksCompleted() {
        // NOTE: Wait till all queue becomes empty
        while ((batcher != null && !batcher.isEmpty()) ||
                (pendingFutures != null && pendingFutures.size() > 0) ||
                (downloadsToInsert != null && !downloadsToInsert.isEmpty())) {

            // Wait for batcher (inbox) completed
            waitBatcherCompleted();

            // wait for pending featurs completed
            waitPendingFuturesCompleted();

            // wait for downloadToInsert batcher completed
            waitDownloadsToInsertBatcherCompleted();
        }
    }

    protected void waitDownloadsToInsertBatcherCompleted() {
        waitBatcherCompleted(downloadsToInsert);
    }

    @Override
    public boolean shouldCreateTarget() {
        return false;
    }

    @Override
    public void setCreateTarget(boolean createTarget) {
        // silently ignore this -- doesn't make sense for pull replicator
    }

    @Override
    protected void goOffline() {
        super.goOffline();

        // stop change tracker
        if (changeTracker != null) {
            changeTracker.stop();
        }
    }

    protected void pauseOrResume() {
        int pending = batcher.count() + pendingSequences.count();
        changeTracker.setPaused(pending >= MAX_PENDING_DOCS);
    }

    @Override
    public String toString() {
        if (str == null) {
            String maskedRemote = "unknown";
            if (remote != null)
                remote.toExternalForm();
            maskedRemote = maskedRemote.replaceAll("://.*:.*@", "://---:---@");
            String type = isPull() ? "pull" : "push";
            String replicationIdentifier = Utils.shortenString(remoteCheckpointDocID(), 5);
            if (replicationIdentifier == null)
                replicationIdentifier = "unknown";
            str = String.format(Locale.ENGLISH, "PullerInternal{%s, %s, %s}",
                    maskedRemote, type, replicationIdentifier);
        }
        return str;
    }
}
