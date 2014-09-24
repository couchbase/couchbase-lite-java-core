package com.couchbase.lite.replicator;

import com.couchbase.lite.AsyncTask;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Misc;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.internal.Body;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.support.BatchProcessor;
import com.couchbase.lite.support.Batcher;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.support.SequenceMap;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;

import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @exclude
 */
@InterfaceAudience.Private
public final class Puller extends Replication implements ChangeTrackerClient {

    private static final int MAX_OPEN_HTTP_CONNECTIONS = 16;
    // Maximum number of revs to fetch in a single bulk request
    public static final int MAX_REVS_TO_GET_IN_BULK = 50;

    // Maximum number of revision IDs to pass in an "?atts_since=" query param
    public static final int MAX_NUMBER_OF_ATTS_SINCE = 50;

    // Does the server support _bulk_get requests?
    protected Boolean canBulkGet;

    // Have I received all current _changes entries?
    protected AtomicBoolean caughtUp;

    protected Batcher<RevisionInternal> downloadsToInsert;
    protected List<RevisionInternal> revsToPull;
    protected List<RevisionInternal> deletedRevsToPull;
    protected List<RevisionInternal> bulkRevsToPull;
    protected ChangeTracker changeTracker;
    protected SequenceMap pendingSequences;
    protected volatile int httpConnectionCount;

    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ public Puller(Database db, URL remote, boolean continuous, ScheduledExecutorService workExecutor) {
        this(db, remote, continuous, null, workExecutor);
    }

    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ public Puller(Database db, URL remote, boolean continuous, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor) {
        super(db, remote, continuous, clientFactory, workExecutor);
    }

    @Override
    @InterfaceAudience.Public
    public boolean isPull() {
        return true;
    }

    @Override
    @InterfaceAudience.Public
    public boolean shouldCreateTarget() {
        return false;
    }

    @Override
    @InterfaceAudience.Public
    public void setCreateTarget(boolean createTarget) {
    }

    @Override
    @InterfaceAudience.Public
    public void stop() {

        if (!running) {
            return;
        }

        // Creating a local copy stops any situations where between the null check and the call to stop
        // that changeTracker could be set to null and so cause a null pointer exception.
        ChangeTracker localCopyOfChangeTracker = changeTracker;
        // TODO: Setting changeTracker to null like this is inherently not thread safe unless we are going to re-edit
        // the puller file to use the 'local' trick above. I don't think that's a good idea. Rather we should restructure
        // puller to be inherently thread safe. But in the meantime the code below replicates the existing functionality.
        changeTracker = null;
        if (localCopyOfChangeTracker != null) {
            Log.d(Log.TAG_SYNC, "%s: stopping changetracker", this, localCopyOfChangeTracker);
            localCopyOfChangeTracker.stop();
            if (!continuous) {
                Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s : puller.stop() calling asyncTaskFinished()", this, Thread.currentThread());
                asyncTaskFinished(1);  // balances asyncTaskStarted() in beginReplicating()
            }
        }

        synchronized (this) {
            revsToPull = null;
            deletedRevsToPull = null;
            bulkRevsToPull = null;
        }

        super.stop();

        if (downloadsToInsert != null) {
            downloadsToInsert.flush();
        }
    }

    /**
     * This is called if I've gone idle but some revisions failed to be pulled.
     * I should start the _changes feed over again, so I can retry all the revisions.
     */
    @InterfaceAudience.Private
    protected void retry() {
        super.retry();

        if (changeTracker != null) {
            changeTracker.stop();
        }
        beginReplicating();
    }

    @Override
    @InterfaceAudience.Private
    public void beginReplicating() {

        if (downloadsToInsert == null) {
            int capacity = 200;
            int delay = 1000;
            downloadsToInsert = new Batcher<RevisionInternal>(workExecutor, capacity, delay, new BatchProcessor<RevisionInternal>() {
                @Override
                public void process(List<RevisionInternal> inbox) {
                    insertDownloads(inbox);
                }
            });
        }
        if (pendingSequences == null) {
            pendingSequences = new SequenceMap();
            if (getLastSequence() != null) {
                // Prime _pendingSequences so its checkpointedValue will reflect the last known seq:
                long seq = pendingSequences.addValue(getLastSequence());
                pendingSequences.removeSequence(seq);
                assert (pendingSequences.getCheckpointedValue().equals(getLastSequence()));

            }
        }

        caughtUp = new AtomicBoolean(false);


        ChangeTracker.ChangeTrackerMode changeTrackerMode;

        // it always starts out as OneShot, but if its a continuous replication
        // it will switch to longpoll later.
        changeTrackerMode = ChangeTracker.ChangeTrackerMode.OneShot;

        Log.w(Log.TAG_SYNC, "%s: starting ChangeTracker with since=%s mode=%s", this, lastSequence, changeTrackerMode);
        boolean usePOST = serverIsSyncGatewayVersion("0.93");

        Map<String, Object> filterParams = null;

        if (documentIDs != null && documentIDs.size() > 0) {
            changeTracker = new ChangeTracker(remote, changeTrackerMode, true, lastSequence, this, usePOST,
                    documentIDs, requestHeaders, getAuthenticator(), isContinuous());
        } else {
            changeTracker = new ChangeTracker(remote, changeTrackerMode, true, lastSequence, this, usePOST,
                    filterName, filterParams, requestHeaders, getAuthenticator(), isContinuous());
        }
        Log.w(Log.TAG_SYNC, "%s: started ChangeTracker %s", this, changeTracker);

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: beginReplicating() calling asyncTaskStarted()", this, Thread.currentThread());
        asyncTaskStarted();

        changeTracker.start();
        if (!continuous) {
            Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: beginReplicating() calling asyncTaskStarted()", this, Thread.currentThread());
            asyncTaskStarted();
        }

    }


    @Override
    @InterfaceAudience.Private
    protected void stopped() {
        downloadsToInsert = null;
        super.stopped();
    }

    // Got a _changes feed entry from the ChangeTracker.
    @Override
    @InterfaceAudience.Private
    public void changeTrackerReceivedChange(Map<String, Object> change) {

        String lastSequence = change.get("seq").toString();
        String docID = (String) change.get("id");
        if (docID == null) {
            return;
        }

        if (!Database.isValidDocumentId(docID)) {
            Log.w(Log.TAG_SYNC, "%s: Received invalid doc ID from _changes: %s", this, change);
            return;
        }
        boolean deleted = (change.containsKey("deleted") && ((Boolean) change.get("deleted")).equals(Boolean.TRUE));
        List<Map<String, Object>> changes = (List<Map<String, Object>>) change.get("changes");
        for (Map<String, Object> changeDict : changes) {
            String revID = (String) changeDict.get("rev");
            if (revID == null) {
                continue;
            }
            PulledRevision rev = new PulledRevision(docID, revID, deleted, db);
            rev.setRemoteSequenceID(lastSequence);
            Log.d(Log.TAG_SYNC, "%s: adding rev to inbox %s", this, rev);

            Log.v(Log.TAG_SYNC, "%s: changeTrackerReceivedChange() incrementing changesCount by 1", this);

            // this is purposefully done slightly different than the ios version
            addToChangesCount(1);

            addToInbox(rev);
        }

        while (revsToPull != null && revsToPull.size() > 1000) {
            try {
                Thread.sleep(500);  // <-- TODO: why is this here?
            } catch (InterruptedException e) {

            }
        }
    }

    @InterfaceAudience.Private
    public void changeTrackerCaughtUp() {
        if (!caughtUp.get()) {
            Log.i(Log.TAG_SYNC, "%s: Caught up with changes!", this);
            boolean success = caughtUp.compareAndSet(false, true);
            if (!success) {
                caughtUp.set(true);
                Log.w(Log.TAG_SYNC, "%s: set caughtUp via CAS failed, force-set to true", this);
            }
            Log.w(Log.TAG_SYNC_ASYNC_TASK, "%s: ChangeTracker changeTrackerCaughtUp() calling asyncTaskFinished", this);
            asyncTaskFinished(1);  // balances -asyncTaskStarted in -beginReplicating
        }
    }

    @Override
    @InterfaceAudience.Private
    public void changeTrackerFinished(ChangeTracker tracker) {
        changeTrackerCaughtUp();
    }

    @Override
    @InterfaceAudience.Private
    public void changeTrackerStopped(ChangeTracker tracker) {
        //TODO: This is the equivalent of the existing behavior for stopping the puller but it is not thread
        //safe, but neither is the existing puller so nothing has been made worse.
        if (changeTracker == null) {
            return;
        }

        Log.w(Log.TAG_SYNC, "%s: ChangeTracker %s stopped", this, tracker);
        if (error == null && tracker.getLastError() != null) {
            setError(tracker.getLastError());
        }
        changeTracker = null;

        if (batcher != null) {
            Log.d(Log.TAG_SYNC, "%s: calling batcher.flush().  batcher.count() is %d", this, batcher.count());
            batcher.flush();
        }
        if (!isContinuous()) {
            // the asyncTaskFinished needs to run on the work executor
            // in order to fix https://github.com/couchbase/couchbase-lite-java-core/issues/91
            // basically, bad things happen when this runs on ChangeTracker thread.
            workExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: changeTrackerStopped() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);  // balances -asyncTaskStarted in -startChangeTracker
                }
            });
        }

        if (!caughtUp.get()) {
            // running this on the workExecutor, because assuming it needs to be according
            // to comments above in the if (!isContinuous()) code block.
            workExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: changeTrackerStopped() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);  // balances -asyncTaskStarted in -beginReplicating
                }
            });
        }

    }

    @Override
    @InterfaceAudience.Private
    public HttpClient getHttpClient() {
        HttpClient httpClient = this.clientFactory.getHttpClient();

        return httpClient;
    }

    /**
     * Process a bunch of remote revisions from the _changes feed at once
     */
    @Override
    @InterfaceAudience.Private
    protected void processInbox(RevisionList inbox) {
        if (canBulkGet == null) {
            canBulkGet = serverIsSyncGatewayVersion("0.81");
        }

        // Ask the local database which of the revs are not known to it:
        String lastInboxSequence = ((PulledRevision) inbox.get(inbox.size() - 1)).getRemoteSequenceID();

        int numRevisionsRemoved = 0;
        try {
            // findMissingRevisions is the local equivalent of _revs_diff. it looks at the
            // array of revisions in ‘inbox’ and removes the ones that already exist. So whatever’s left in ‘inbox’
            // afterwards are the revisions that need to be downloaded.
            numRevisionsRemoved = db.findMissingRevisions(inbox);
        } catch (SQLException e) {
            Log.e(Log.TAG_SYNC, String.format("%s failed to look up local revs", this), e);
            inbox = null;
        }

        //introducing this to java version since inbox may now be null everywhere
        int inboxCount = 0;
        if (inbox != null)
            inboxCount = inbox.size();

        if (numRevisionsRemoved > 0) {
            Log.v(Log.TAG_SYNC, "%s: processInbox() setting changesCount to: %s", this, getChangesCount() - numRevisionsRemoved);
            // May decrease the changesCount, to account for the revisions we just found out we don’t need to get.
            addToChangesCount(-1 * numRevisionsRemoved);
        }

        if (inboxCount == 0) {
            // Nothing to do. Just bump the lastSequence.
            Log.w(Log.TAG_SYNC, "%s no new remote revisions to fetch", this);
            long seq = pendingSequences.addValue(lastInboxSequence);
            pendingSequences.removeSequence(seq);
            setLastSequence(pendingSequences.getCheckpointedValue());
            return;
        }

        Log.v(Log.TAG_SYNC, "%s: fetching %s remote revisions...", this, inboxCount);

        // Dump the revs into the queue of revs to pull from the remote db:
        synchronized (this) {
            int numBulked = 0;


            for (int i = 0; i < inbox.size(); i++) {
                PulledRevision rev = (PulledRevision) inbox.get(i);

                //TODO: add support for rev isConflicted
                if (canBulkGet || (rev.getGeneration() == 1 && !rev.isDeleted())) { // &&!rev.isConflicted)

                    //optimistically pull 1st-gen revs in bulk
                    if (bulkRevsToPull == null)
                        bulkRevsToPull = new ArrayList<RevisionInternal>(100);

                    bulkRevsToPull.add(rev);

                    ++numBulked;
                } else {
                    queueRemoteRevision(rev);
                }

                rev.setSequence(pendingSequences.addValue(rev.getRemoteSequenceID()));
            }
        }
        pullRemoteRevisions();
    }


    /**
     * Add a revision to the appropriate queue of revs to individually GET
     */
    @InterfaceAudience.Private
    protected void queueRemoteRevision(RevisionInternal rev) {
        if (rev.isDeleted()) {
            if (deletedRevsToPull == null) {
                deletedRevsToPull = new ArrayList<RevisionInternal>(100);
            }

            deletedRevsToPull.add(rev);
        } else {
            if (revsToPull == null)
                revsToPull = new ArrayList<RevisionInternal>(100);
            revsToPull.add(rev);
        }
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
        synchronized (this) {
            while (httpConnectionCount + workToStartNow.size() < MAX_OPEN_HTTP_CONNECTIONS) {
                int nBulk = 0;
                if (bulkRevsToPull != null) {
                    nBulk = (bulkRevsToPull.size() < MAX_REVS_TO_GET_IN_BULK) ? bulkRevsToPull.size() : MAX_REVS_TO_GET_IN_BULK;
                }
                if (nBulk == 1) {
                    // Rather than pulling a single revision in 'bulk', just pull it normally:
                    queueRemoteRevision(bulkRevsToPull.get(0));
                    bulkRevsToPull.remove(0);
                    nBulk = 0;
                }
                if (nBulk > 0) {
                    // Prefer to pull bulk revisions:
                    bulkWorkToStartNow.addAll(bulkRevsToPull.subList(0, nBulk));
                    bulkRevsToPull.subList(0, nBulk).clear();
                } else {
                    // Prefer to pull an existing revision over a deleted one:
                    List<RevisionInternal> queue = revsToPull;
                    if (queue == null || queue.size() == 0) {
                        queue = deletedRevsToPull;
                        if (queue == null || queue.size() == 0)
                            break;  // both queues are empty
                    }
                    workToStartNow.add(queue.get(0));
                    queue.remove(0);
                }
            }
        }

        //actually run it outside the synchronized block
        if(bulkWorkToStartNow.size() > 0) {
            pullBulkRevisions(bulkWorkToStartNow);
        }

        for (RevisionInternal work : workToStartNow) {
            pullRemoteRevision(work);
        }
    }


    /**
     * Fetches the contents of a revision from the remote db, including its parent revision ID.
     * The contents are stored into rev.properties.
     */
    @InterfaceAudience.Private
    public void pullRemoteRevision(final RevisionInternal rev) {

        Log.d(Log.TAG_SYNC, "%s: pullRemoteRevision with rev: %s", this, rev);

        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: pullRemoteRevision() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        ++httpConnectionCount;

        // Construct a query. We want the revision history, and the bodies of attachments that have
        // been added since the latest revisions we have locally.
        // See: http://wiki.apache.org/couchdb/HTTP_Document_API#Getting_Attachments_With_a_Document
        StringBuilder path = new StringBuilder("/" + URLEncoder.encode(rev.getDocId()) + "?rev=" + URLEncoder.encode(rev.getRevId()) + "&revs=true&attachments=true");
        List<String> knownRevs = knownCurrentRevIDs(rev);
        if (knownRevs == null) {
            Log.w(Log.TAG_SYNC, "knownRevs == null, something is wrong, possibly the replicator has shut down");
            Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: pullRemoteRevision() calling asyncTaskFinished()", this, Thread.currentThread());
            asyncTaskFinished(1);
            --httpConnectionCount;
            return;
        }
        if (knownRevs.size() > 0) {
            path.append("&atts_since=");
            path.append(joinQuotedEscaped(knownRevs));
        }

        //create a final version of this variable for the log statement inside
        //FIXME find a way to avoid this
        final String pathInside = path.toString();
        sendAsyncMultipartDownloaderRequest("GET", pathInside, null, db, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(Object result, Throwable e) {
                try {
                    if (e != null) {
                        Log.e(Log.TAG_SYNC, "Error pulling remote revision", e);
                        revisionFailed(rev, e);
                    } else {
                        Map<String, Object> properties = (Map<String, Object>) result;
                        PulledRevision gotRev = new PulledRevision(properties, db);
                        gotRev.setSequence(rev.getSequence());
                        // Add to batcher ... eventually it will be fed to -insertDownloads:.
                        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: pullRemoteRevision.sendAsyncMultipartDownloaderRequest() calling asyncTaskStarted()", this, Thread.currentThread());

                        asyncTaskStarted();
                        // TODO: [gotRev.body compact];
                        Log.d(Log.TAG_SYNC, "%s: pullRemoteRevision add rev: %s to batcher: %s", Puller.this, gotRev, downloadsToInsert);
                        downloadsToInsert.queueObject(gotRev);
                    }
                } finally {
                    Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: pullRemoteRevision.sendAsyncMultipartDownloaderRequest() calling asyncTaskFinished()", this, Thread.currentThread());

                    asyncTaskFinished(1);
                }

                // Note that we've finished this task; then start another one if there
                // are still revisions waiting to be pulled:
                --httpConnectionCount;
                pullRemoteRevisions();
            }
        });
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
        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: pullBulkRevisions() calling asyncTaskStarted()", this, Thread.currentThread());
        asyncTaskStarted();
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
                                rev = new RevisionInternal(props, db);
                            } else {
                                rev = new RevisionInternal((String) props.get("id"), (String) props.get("rev"), false, db);
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

                        public void onCompletion(Object result, Throwable e) {
                            // The entire _bulk_get is finished:
                            if (e != null) {
                                setError(e);
                                revisionFailed();
                                completedChangesCount.addAndGet(remainingRevs.size());
                            }
                            // Note that we've finished this task:
                            Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: pullBulkRevisions.RemoteRequestCompletionBlock() calling asyncTaskFinished()", this, Thread.currentThread());

                            asyncTaskFinished(1);
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

        remoteRequestExecutor.execute(dl);

    }


    // Get as many revisions as possible in one _all_docs request.
    // This is compatible with CouchDB, but it only works for revs of generation 1 without attachments.

    protected void pullBulkWithAllDocs(final List<RevisionInternal> bulkRevs) {
        // http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: pullBulkWithAllDocs() calling asyncTaskStarted()", this, Thread.currentThread());
        asyncTaskStarted();
        ++httpConnectionCount;
        final RevisionList remainingRevs = new RevisionList(bulkRevs);

        Collection<String> keys = CollectionUtils.transform(bulkRevs,
                new CollectionUtils.Functor<RevisionInternal, String>() {
                    public String invoke(RevisionInternal rev) {
                        return rev.getDocId();
                    }
                }
        );

        Map<String, Object> body = new HashMap<String, Object>();
        body.put("keys", keys);

        sendAsyncRequest("POST",
                "/_all_docs?include_docs=true",
                body,
                new RemoteRequestCompletionBlock() {

                    public void onCompletion(Object result, Throwable e) {

                        Map<String, Object> res = (Map<String, Object>) result;

                        if (e != null) {
                            setError(e);
                            revisionFailed();
                            completedChangesCount.addAndGet(bulkRevs.size());
                        } else {
                            // Process the resulting rows' documents.
                            // We only add a document if it doesn't have attachments, and if its
                            // revID matches the one we asked for.
                            List<Map<String, Object>> rows = (List<Map<String, Object>>) res.get("rows");
                            Log.v(Log.TAG_SYNC, "%s checking %d bulk-fetched remote revisions", this, rows.size());
                            for (Map<String, Object> row : rows) {
                                Map<String, Object> doc = (Map<String, Object>) row.get("doc");
                                if (doc != null && doc.get("_attachments") == null) {
                                    RevisionInternal rev = new RevisionInternal(doc, db);
                                    RevisionInternal removedRev = remainingRevs.removeAndReturnRev(rev);
                                    if (removedRev != null) {
                                        rev.setSequence(removedRev.getSequence());
                                        queueDownloadedRevision(rev);
                                    }
                                } else {
                                    Status status = statusFromBulkDocsResponseItem(row);
                                    if (status.isError() && row.containsKey("key") && row.get("key") != null) {
                                        RevisionInternal rev = remainingRevs.revWithDocId((String)row.get("key"));
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
                            Log.v(Log.TAG_SYNC, "%s bulk-fetch didn't work for %d of %d revs; getting individually", this, remainingRevs.size(), bulkRevs.size());
                            for (RevisionInternal rev : remainingRevs) {
                                queueRemoteRevision(rev);
                            }
                            pullRemoteRevisions();
                        }

                        // Note that we've finished this task:
                        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: pullBulkWithAllDocs() calling asyncTaskFinished()", this, Thread.currentThread());

                        asyncTaskFinished(1);
                        --httpConnectionCount;
                        // Start another task if there are still revisions waiting to be pulled:
                        pullRemoteRevisions();
                    }
                });
    }


    // This invokes the tranformation block if one is installed and queues the resulting CBL_Revision
    private void queueDownloadedRevision(RevisionInternal rev) {

        if (revisionBodyTransformationBlock != null) {
            // Add 'file' properties to attachments pointing to their bodies:

            for (Map.Entry<String, Map<String, Object>> entry : ((Map<String, Map<String, Object>>) rev.getProperties().get("_attachments")).entrySet()) {
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
                return;
            }
            rev = xformed;

            // Clean up afterwards
            Map<String, Object> attachments = (Map<String, Object>) rev.getProperties().get("_attachments");

            for (Map.Entry<String, Map<String, Object>> entry : ((Map<String, Map<String, Object>>) rev.getProperties().get("_attachments")).entrySet()) {
                Map<String, Object> attachment = entry.getValue();
                attachment.remove("file");
            }
        }

        //TODO: rev.getBody().compact();
        Log.v(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: queueDownloadedRevision() calling asyncTaskStarted()", this, Thread.currentThread());

        asyncTaskStarted();
        downloadsToInsert.queueObject(rev);

    }


    /**
     * This will be called when _revsToInsert fills up:
     */
    @InterfaceAudience.Private
    public void insertDownloads(List<RevisionInternal> downloads) {

        Log.i(Log.TAG_SYNC, this + " inserting " + downloads.size() + " revisions...");
        long time = System.currentTimeMillis();
        Collections.sort(downloads, getRevisionListComparator());

        db.beginTransaction();
        boolean success = false;
        try {
            for (RevisionInternal rev : downloads) {
                long fakeSequence = rev.getSequence();
                List<String> history = db.parseCouchDBRevisionHistory(rev.getProperties());
                if (history.isEmpty() && rev.getGeneration() > 1) {
                    Log.w(Log.TAG_SYNC, "%s: Missing revision history in response for: %s", this, rev);
                    setError(new CouchbaseLiteException(Status.UPSTREAM_ERROR));
                    revisionFailed();
                    continue;
                }

                Log.v(Log.TAG_SYNC, "%s: inserting %s %s", this, rev.getDocId(), history);

                // Insert the revision
                try {
                    db.forceInsert(rev, history, remote);
                } catch (CouchbaseLiteException e) {
                    if (e.getCBLStatus().getCode() == Status.FORBIDDEN) {
                        Log.i(Log.TAG_SYNC, "%s: Remote rev failed validation: %s", this, rev);
                    } else {
                        Log.w(Log.TAG_SYNC, "%s: failed to write %s: status=%s", this, rev, e.getCBLStatus().getCode());
                        revisionFailed();
                        setError(new HttpResponseException(e.getCBLStatus().getCode(), null));
                        continue;
                    }
                }

                // Mark this revision's fake sequence as processed:
                pendingSequences.removeSequence(fakeSequence);

            }

            Log.v(Log.TAG_SYNC, "%s: finished inserting %d revisions", this, downloads.size());
            success = true;

        } catch (SQLException e) {
            Log.e(Log.TAG_SYNC, this + ": Exception inserting revisions", e);
        } finally {
            db.endTransaction(success);

            if (success) {

                // Checkpoint:
                setLastSequence(pendingSequences.getCheckpointedValue());

                long delta = System.currentTimeMillis() - time;
                Log.v(Log.TAG_SYNC, "%s: inserted %d revs in %d milliseconds", this, downloads.size(), delta);

                int newCompletedChangesCount = getCompletedChangesCount() + downloads.size();
                Log.d(Log.TAG_SYNC, "%s insertDownloads() updating completedChangesCount from %d -> %d ", this, getCompletedChangesCount(), newCompletedChangesCount);

                addToCompletedChangesCount(downloads.size());

            }

            Log.d(Log.TAG_SYNC_ASYNC_TASK, "%s | %s: insertDownloads() calling asyncTaskFinished() with value: %d", this, Thread.currentThread(), downloads.size());

            asyncTaskFinished(downloads.size());
        }


    }

    @InterfaceAudience.Private
    private Comparator<RevisionInternal> getRevisionListComparator() {
        return new Comparator<RevisionInternal>() {

            public int compare(RevisionInternal reva, RevisionInternal revb) {
                return Misc.TDSequenceCompare(reva.getSequence(), revb.getSequence());
            }

        };
    }

    @InterfaceAudience.Private
    /* package */
    List<String> knownCurrentRevIDs(RevisionInternal rev) {
        if (db != null) {
            return db.getAllRevisionsOfDocumentID(rev.getDocId(), true).getAllRevIds();
        }
        return null;
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
            Log.w(Log.TAG_SYNC, "Unable to serialize json", e);
        }
        return URLEncoder.encode(new String(json));
    }


    @InterfaceAudience.Public
    public boolean goOffline() {

        Log.d(Log.TAG_SYNC, "%s: goOffline() called", this);
        if (!super.goOffline()) {
            return false;
        }

        if (changeTracker != null) {
            db.runAsync(new AsyncTask() {
                @Override
                public void run(Database database) {
                    Log.d(Log.TAG_SYNC, "%s: stopping changeTracker: %s", this, changeTracker);
                    changeTracker.stop();
                }
            });
        }

        return true;
    }

    private void revisionFailed(RevisionInternal rev, Throwable throwable) {
        if (Utils.isTransientError(throwable)) {
            revisionFailed();  // retry later
        } else {
            Log.v(Log.TAG_SYNC, "%s: giving up on %s: %s", this, rev, throwable);
            pendingSequences.removeSequence(rev.getSequence());
        }
        completedChangesCount.getAndIncrement();
    }

}

/**
 * A revision received from a remote server during a pull. Tracks the opaque remote sequence ID.
 */
@InterfaceAudience.Private
class PulledRevision extends RevisionInternal {

    public PulledRevision(Body body, Database database) {
        super(body, database);
    }

    public PulledRevision(String docId, String revId, boolean deleted, Database database) {
        super(docId, revId, deleted, database);
    }

    public PulledRevision(Map<String, Object> properties, Database database) {
        super(properties, database);
    }

    protected String remoteSequenceID;

    public String getRemoteSequenceID() {
        return remoteSequenceID;
    }

    public void setRemoteSequenceID(String remoteSequenceID) {
        this.remoteSequenceID = remoteSequenceID;
    }

}
