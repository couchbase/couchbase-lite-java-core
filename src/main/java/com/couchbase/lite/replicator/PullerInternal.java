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
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.support.SequenceMap;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.URIUtils;
import com.couchbase.lite.util.Utils;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pull Replication
 *
 * @exclude
 */
@InterfaceAudience.Private
public class PullerInternal extends ReplicationInternal implements ChangeTrackerClient {

    private static final int MAX_OPEN_HTTP_CONNECTIONS = 16;

    // Maximum number of revs to fetch in a single bulk request
    public static final int MAX_REVS_TO_GET_IN_BULK = 50;

    // Maximum number of revision IDs to pass in an "?atts_since=" query param
    public static final int MAX_NUMBER_OF_ATTS_SINCE = 50;

    public static int CHANGE_TRACKER_RESTART_DELAY_MS = 10 * 1000;

    public static final int MAX_PENDING_DOCS = 200;

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

    // for waitingPendingFutures
    protected boolean waitingForPendingFutures = false;
    protected Object lockWaitForPendingFutures = new Object();

    public PullerInternal(Database db,
                          URL remote,
                          HttpClientFactory clientFactory,
                          ScheduledExecutorService workExecutor,
                          Replication.Lifecycle lifecycle,
                          Replication parentReplication) {
        super(db, remote, clientFactory, workExecutor, lifecycle, parentReplication);
    }

    /**
     * Actual work of starting the replication process.
     */
    protected void beginReplicating() {
        Log.d(Log.TAG_SYNC, "startReplicating()");

        initPendingSequences();

        initDownloadsToInsert();

        startChangeTracker();

        // start replicator ..
    }

    private void initDownloadsToInsert() {
        if (downloadsToInsert == null) {
            int capacity = 200;
            int delay = 1000;
            downloadsToInsert = new Batcher<RevisionInternal>(workExecutor,
                    capacity, delay, new BatchProcessor<RevisionInternal>() {
                @Override
                public void process(List<RevisionInternal> inbox) {
                    insertDownloads(inbox);
                }
            });
        }
    }

    public boolean isPull() {
        return true;
    }

    protected void maybeCreateRemoteDB() {
        // puller never needs to do this
    }

    protected void startChangeTracker() {

        ChangeTracker.ChangeTrackerMode changeTrackerMode;

        // it always starts out as OneShot, but if its a continuous replication
        // it will switch to longpoll later.
        changeTrackerMode = ChangeTracker.ChangeTrackerMode.OneShot;

        Log.d(Log.TAG_SYNC, "%s: starting ChangeTracker with since=%s mode=%s",
                this, lastSequence, changeTrackerMode);
        changeTracker = new ChangeTracker(remote, changeTrackerMode, true, lastSequence, this);
        changeTracker.setAuthenticator(getAuthenticator());
        Log.d(Log.TAG_SYNC, "%s: started ChangeTracker %s", this, changeTracker);

        if (filterName != null) {
            changeTracker.setFilterName(filterName);
            if (filterParams != null) {
                changeTracker.setFilterParams(filterParams);
            }
        }
        changeTracker.setDocIDs(documentIDs);
        changeTracker.setRequestHeaders(requestHeaders);
        changeTracker.setContinuous(lifecycle == Replication.Lifecycle.CONTINUOUS);

        changeTracker.setUsePOST(serverIsSyncGatewayVersion("0.93"));
        changeTracker.start();
    }

    /**
     * Process a bunch of remote revisions from the _changes feed at once
     */
    @Override
    @InterfaceAudience.Private
    protected void processInbox(RevisionList inbox) {
        Log.d(Log.TAG_SYNC, "processInbox called");

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
            Log.e(Log.TAG_SYNC, String.format("%s failed to look up local revs", this), e);
            inbox = null;
        }

        //introducing this to java version since inbox may now be null everywhere
        int inboxCount = 0;
        if (inbox != null) {
            inboxCount = inbox.size();
        }

        if (numRevisionsRemoved > 0) {
            Log.v(Log.TAG_SYNC, "%s: processInbox() setting changesCount to: %s",
                    this, getChangesCount().get() - numRevisionsRemoved);
            // May decrease the changesCount, to account for the revisions we just found out we don't need to get.
            addToChangesCount(-1 * numRevisionsRemoved);
        }

        if (inboxCount == 0) {
            // Nothing to do. Just bump the lastSequence.
            Log.w(Log.TAG_SYNC,
                    "%s no new remote revisions to fetch.  add lastInboxSequence (%s) to pendingSequences (%s)",
                    this, lastInboxSequence, pendingSequences);
            long seq = pendingSequences.addValue(lastInboxSequence);
            pendingSequences.removeSequence(seq);
            setLastSequence(pendingSequences.getCheckpointedValue());
            pauseOrResume();
            return;
        }

        Log.v(Log.TAG_SYNC, "%s: fetching %s remote revisions...", this, inboxCount);

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
        Log.v(Log.TAG_SYNC, "%s bulk-fetching %d remote revisions...", this, nRevs);
        Log.v(Log.TAG_SYNC, "%s bulk-fetching remote revisions: %s", this, bulkRevs);

        if (!canBulkGet) {
            pullBulkWithAllDocs(bulkRevs);
            return;
        }

        Log.v(Log.TAG_SYNC, "%s: POST _bulk_get", this);
        final List<RevisionInternal> remainingRevs = new ArrayList<RevisionInternal>(bulkRevs);

        ++httpConnectionCount;

        final BulkDownloader dl;
        try {

            dl = new BulkDownloader(workExecutor,
                    clientFactory,
                    remote,
                    bulkRevs,
                    db,
                    this.requestHeaders,
                    new BulkDownloader.BulkDownloaderDocumentBlock() {
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
                                Log.w(Log.TAG_SYNC, "%s : Received unexpected rev rev", this);
                            }

                            if (props.get("_id") != null) {
                                // Add to batcher ... eventually it will be fed to -insertRevisions:.
                                queueDownloadedRevision(rev);
                            } else {
                                Status status = statusFromBulkDocsResponseItem(props);
                                error = new CouchbaseLiteException(status);
                                revisionFailed(rev, error);
                            }
                        }
                    },
                    new RemoteRequestCompletionBlock() {

                        public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
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
            Log.e(Log.TAG_SYNC, "%s: pullBulkRevisions Exception: %s", this, e);
            return;
        }

        dl.setAuthenticator(getAuthenticator());

        // set compressed request - gzip
        dl.setCompressedRequest(canSendCompressedRequests());

        synchronized (remoteRequestExecutor) {
            if (!remoteRequestExecutor.isShutdown()) {
                Future future = remoteRequestExecutor.submit(dl);
                pendingFutures.add(future);
            }
        }
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
                Log.v(Log.TAG_SYNC, "%s: Transformer rejected revision %s", this, rev);
                pendingSequences.removeSequence(rev.getSequence());
                lastSequence = pendingSequences.getCheckpointedValue();
                pauseOrResume();
                return;
            }
            rev = xformed;

            // Clean up afterwards
            Map<String, Object> attachments = (Map<String, Object>) rev.getProperties().get("_attachments");

            for (Map.Entry<String, Map<String, Object>> entry :
                    ((Map<String, Map<String, Object>>) rev.getProperties().get("_attachments")).entrySet()) {
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
                "/_all_docs?include_docs=true",
                body,
                new RemoteRequestCompletionBlock() {

                    public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {

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
                            Log.v(Log.TAG_SYNC, "%s checking %d bulk-fetched remote revisions", this, rows.size());
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
                            Log.v(Log.TAG_SYNC,
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

        Log.d(Log.TAG_SYNC, this + " inserting " + downloads.size() + " revisions...");
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
                            Log.w(Log.TAG_SYNC, "%s: Missing revision history in response for: %s", this, rev);
                            setError(new CouchbaseLiteException(Status.UPSTREAM_ERROR));
                            continue;
                        }

                        Log.v(Log.TAG_SYNC, "%s: inserting %s %s", this, rev.getDocID(), history);

                        // Insert the revision
                        try {
                            db.forceInsert(rev, history, remote);
                        } catch (CouchbaseLiteException e) {
                            if (e.getCBLStatus().getCode() == Status.FORBIDDEN) {
                                Log.i(Log.TAG_SYNC, "%s: Remote rev failed validation: %s", this, rev);
                            } else {
                                Log.w(Log.TAG_SYNC, "%s: failed to write %s: status=%s",
                                        this, rev, e.getCBLStatus().getCode());
                                setError(new HttpResponseException(e.getCBLStatus().getCode(), null));
                                continue;
                            }
                        }

                        //if(rev.getBody() != null) rev.getBody().release();
                        if (rev.getBody() != null) rev.getBody().compact();

                        // Mark this revision's fake sequence as processed:
                        pendingSequences.removeSequence(fakeSequence);
                    }

                    Log.v(Log.TAG_SYNC, "%s: finished inserting %d revisions", this, downloads.size());
                    success = true;

                } catch (SQLException e) {
                    Log.e(Log.TAG_SYNC, this + ": Exception inserting revisions", e);
                } finally {
                    if (success) {

                        // Checkpoint:
                        setLastSequence(pendingSequences.getCheckpointedValue());

                        long delta = System.currentTimeMillis() - time;
                        Log.v(Log.TAG_SYNC,
                                "%s: inserted %d revs in %d milliseconds",
                                this, downloads.size(), delta);

                        int newCompletedChangesCount = getCompletedChangesCount().get() + downloads.size();
                        Log.d(Log.TAG_SYNC,
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
    private Comparator<RevisionInternal> getRevisionListComparator() {
        return new Comparator<RevisionInternal>() {

            public int compare(RevisionInternal reva, RevisionInternal revb) {
                return Misc.SequenceCompare(reva.getSequence(), revb.getSequence());
            }

        };
    }

    private void revisionFailed(RevisionInternal rev, Throwable throwable) {
        if (!Utils.isTransientError(throwable)) {
            Log.v(Log.TAG_SYNC, "%s: giving up on %s: %s", this, rev, throwable);
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

        Log.d(Log.TAG_SYNC, "%s: pullRemoteRevision with rev: %s", this, rev);

        ++httpConnectionCount;

        // Construct a query. We want the revision history, and the bodies of attachments that have
        // been added since the latest revisions we have locally.
        // See: http://wiki.apache.org/couchdb/HTTP_Document_API#Getting_Attachments_With_a_Document
        StringBuilder path = new StringBuilder("/");
        path.append(encodeDocumentId(rev.getDocID()));
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
                null, db, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                if (e != null) {
                    Log.e(Log.TAG_SYNC, "Error pulling remote revision", e);
                    revisionFailed(rev, e);
                } else {
                    Map<String, Object> properties = (Map<String, Object>) result;
                    PulledRevision gotRev = new PulledRevision(properties);
                    gotRev.setSequence(rev.getSequence());

                    Log.d(Log.TAG_SYNC, "%s: pullRemoteRevision add rev: %s to batcher: %s",
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
    public String joinQuotedEscaped(List<String> strings) {
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

    @Override
    public HttpClient getHttpClient() {

        HttpClient httpClient = this.clientFactory.getHttpClient();

        return httpClient;
    }

    @Override
    public void changeTrackerReceivedChange(final Map<String, Object> change) {
        try {
            Log.d(Log.TAG_SYNC, "changeTrackerReceivedChange: %s", change);
            processChangeTrackerChange(change);
        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, "Error processChangeTrackerChange(): %s", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * in CBL_Puller.m
     * - (void) changeTrackerReceivedSequence: (id)remoteSequenceID
     * docID: (NSString*)docID
     * revIDs: (NSArray*)revIDs
     * deleted: (BOOL)deleted
     */
    protected void processChangeTrackerChange(final Map<String, Object> change) {
        String lastSequence = change.get("seq").toString();
        String docID = (String) change.get("id");
        if (docID == null) {
            return;
        }

        if (!Document.isValidDocumentId(docID)) {
            Log.w(Log.TAG_SYNC, "%s: Received invalid doc ID from _changes: %s", this, change);
            return;
        }
        boolean deleted = (change.containsKey("deleted") &&
                ((Boolean) change.get("deleted")).equals(Boolean.TRUE));
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

            if(changes.size() > 1)
                rev.setConflicted(true);

            Log.d(Log.TAG_SYNC, "%s: adding rev to inbox %s", this, rev);

            Log.v(Log.TAG_SYNC,
                    "%s: changeTrackerReceivedChange() incrementing changesCount by 1", this);

            // this is purposefully done slightly different than the ios version
            addToChangesCount(1);

            addToInbox(rev);
        }

        pauseOrResume();
    }

    @Override
    public void changeTrackerStopped(ChangeTracker tracker) {
        // this callback will be on the changetracker thread, but we need
        // to do the work on the replicator thread.
        synchronized (workExecutor) {
            if (!workExecutor.isShutdown()) {
                workExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            processChangeTrackerStopped(changeTracker);
                        } catch (RuntimeException e) {
                            e.printStackTrace();
                            throw e;
                        }
                    }
                });
            }
        }
    }

    private void processChangeTrackerStopped(ChangeTracker tracker) {
        Log.d(Log.TAG_SYNC, "changeTrackerStopped.  lifecycle: %s", lifecycle);
        switch (lifecycle) {
            case ONESHOT:
                // TODO: This is too early to fire STOP_GRACEFUL, Need to change.
                Log.d(Log.TAG_SYNC, "fire STOP_GRACEFUL");
                if (tracker.getLastError() != null) {
                    setError(tracker.getLastError());
                }
                stateMachine.fire(ReplicationTrigger.STOP_GRACEFUL);
                break;
            case CONTINUOUS:
                if (stateMachine.isInState(ReplicationState.OFFLINE)) {
                    // in this case, we don't want to do anything here, since
                    // we told the change tracker to go offline ..
                    Log.d(Log.TAG_SYNC, "Change tracker stopped because we are going offline");
                } else if (stateMachine.isInState(ReplicationState.STOPPING) ||
                        stateMachine.isInState(ReplicationState.STOPPED)) {
                    Log.d(Log.TAG_SYNC, "Change tracker stopped because replicator is stopping or stopped.");
                } else {
                    // otherwise, try to restart the change tracker, since it should
                    // always be running in continuous replications
                    String msg = String.format("Change tracker stopped during continuous replication");
                    Log.e(Log.TAG_SYNC, msg);
                    parentReplication.setLastError(new Exception(msg));
                    fireTrigger(ReplicationTrigger.WAITING_FOR_CHANGES);
                    Log.d(Log.TAG_SYNC, "Scheduling change tracker restart in %d ms", CHANGE_TRACKER_RESTART_DELAY_MS);
                    workExecutor.schedule(new Runnable() {
                        @Override
                        public void run() {
                            // the replication may have been stopped by the time this scheduled fires
                            // so we need to check the state here.
                            if (stateMachine.isInState(ReplicationState.RUNNING)) {
                                Log.d(Log.TAG_SYNC, "%s still running, restarting change tracker", this);
                                startChangeTracker();
                            } else {
                                Log.d(Log.TAG_SYNC, "%s still no longer running, not restarting change tracker", this);
                            }
                        }
                    }, CHANGE_TRACKER_RESTART_DELAY_MS, TimeUnit.MILLISECONDS);
                }
                break;
            default:
                Log.e(Log.TAG_SYNC, "Unknown lifecycle: %s", lifecycle);
        }
    }

    @Override
    public void changeTrackerFinished(ChangeTracker tracker) {
        Log.d(Log.TAG_SYNC, "changeTrackerFinished");
    }

    private void waitForPendingFuturesWithNewThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                waitForPendingFutures();
            }
        }).start();
    }

    @Override
    public void changeTrackerCaughtUp() {
        Log.d(Log.TAG_SYNC, "changeTrackerCaughtUp");
        // for continuous replications, once the change tracker is caught up, we
        // should try to go into the idle state.
        if (isContinuous()) {
            // this has to be on a different thread than the replicator thread, or else it's a deadlock
            // because it might be waiting for jobs that have been scheduled, and not
            // yet executed (and which will never execute because this will block processing).
            waitForPendingFuturesWithNewThread();
        }
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

    protected void stopGraceful() {
        super.stopGraceful();

        Log.d(Log.TAG_SYNC, "PullerInternal.stopGraceful() started");

        // this has to be on a different thread than the replicator thread, or else it's a deadlock
        // because it might be waiting for jobs that have been scheduled, and not
        // yet executed (and which will never execute because this will block processing).
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // wait for all tasks completed
                    waitForAllTasksCompleted();

                    // stop change tracker
                    if (changeTracker != null) {
                        Log.d(Log.TAG_SYNC, "stopping change tracker");
                        changeTracker.stop();
                        Log.d(Log.TAG_SYNC, "stopped change tracker");
                    }
                } catch (Exception e) {
                    Log.e(Log.TAG_SYNC, "stopGraceful.run() had exception: %s", e);
                    e.printStackTrace();
                } finally {
                    // stop replicator immediate
                    triggerStopImmediate();
                }
                Log.d(Log.TAG_SYNC, "PullerInternal stopGraceful.run() finished");
            }
        }).start();
    }

    public void waitForPendingFutures() {
        synchronized (lockWaitForPendingFutures) {
            if (waitingForPendingFutures) {
                return;
            }
            waitingForPendingFutures = true;
        }

        Log.d(Log.TAG_SYNC, "[PullerInternal.waitForPendingFutures()] STARTED - thread id: " + Thread.currentThread().getId());

        try {
            waitForAllTasksCompleted();
        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, "Exception waiting for pending futures: %s", e);
        }

        fireTrigger(ReplicationTrigger.WAITING_FOR_CHANGES);

        Log.d(Log.TAG_SYNC, "[waitForPendingFutures()] END - thread id: " + Thread.currentThread().getId());

        synchronized (lockWaitForPendingFutures) {
            waitingForPendingFutures = false;
        }
    }

    private void waitForAllTasksCompleted() {
        // NOTE: Wait till all queue becomes empty
        while ((batcher != null && batcher.count() > 0) ||
                (pendingFutures != null && pendingFutures.size() > 0) ||
                (downloadsToInsert != null && downloadsToInsert.count() > 0)) {

            // Wait for batcher completed
            if (batcher != null) {
                // if batcher delays task execution, need to wait same amount of time. (0.5 sec or 0 sec)
                try {
                    Thread.sleep(batcher.getDelay());
                } catch (Exception e) {
                }
                Log.d(Log.TAG_SYNC, "batcher.waitForPendingFutures()");
                batcher.waitForPendingFutures();
            }

            // wait for pending featurs completed
            Log.d(Log.TAG_SYNC, "waitPendingFuturesCompleted()");
            waitPendingFuturesCompleted();

            // wait for downloadToInsert batcher completed
            if (downloadsToInsert != null) {
                // if batcher delays task execution, need to wait same amount of time. (1.0 sec or 0 sec)
                try {
                    Thread.sleep(downloadsToInsert.getDelay());
                } catch (Exception e) {
                }
                Log.d(Log.TAG_SYNC, "downloadsToInsert.waitForPendingFutures()");
                downloadsToInsert.waitForPendingFutures();
            }
        }
    }

    private void waitPendingFuturesCompleted() {
        try {
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
        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, "Exception waiting for pending futures: %s", e);
        }
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

        // TODO: stop remote requests in progress, but first
        // TODO: write a test that verifies this actually works
    }

    @Override
    protected void goOnline() {
        super.goOnline();

        // start change tracker
        beginReplicating();
    }

    protected void pauseOrResume() {
        int pending = batcher.count() + pendingSequences.count();
        changeTracker.setPaused(pending >= MAX_PENDING_DOCS);
    }
}
