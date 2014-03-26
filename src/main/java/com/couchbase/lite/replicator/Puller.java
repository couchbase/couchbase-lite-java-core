package com.couchbase.lite.replicator;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Misc;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.internal.Body;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.support.BatchProcessor;
import com.couchbase.lite.support.Batcher;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.support.SequenceMap;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.Log;

import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;

import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;


/**
 * @exclude
 */
@InterfaceAudience.Private
public final class Puller extends Replication implements ChangeTrackerClient {

    private static final int MAX_OPEN_HTTP_CONNECTIONS = 16;

    protected Batcher<RevisionInternal> downloadsToInsert;
    protected List<RevisionInternal> revsToPull;
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
    public void setCreateTarget(boolean createTarget) { }

    @Override
    @InterfaceAudience.Public
    public void stop() {

        if(!running) {
            return;
        }

        if(changeTracker != null) {
            Log.d(Database.TAG, this + ": stopping changetracker " + changeTracker);
            changeTracker.setClient(null);  // stop it from calling my changeTrackerStopped()
            changeTracker.stop();
            changeTracker = null;
            if(!continuous) {
                Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": puller.stop() calling asyncTaskFinished()");
                asyncTaskFinished(1);  // balances asyncTaskStarted() in beginReplicating()
            }
        }

        synchronized(this) {
            revsToPull = null;
        }

        super.stop();

        if(downloadsToInsert != null) {
            downloadsToInsert.flush();
        }
    }


    @Override
    @InterfaceAudience.Private
    public void beginReplicating() {
        if(downloadsToInsert == null) {
            int capacity = 200;
            int delay = 1000;
            downloadsToInsert = new Batcher<RevisionInternal>(workExecutor, capacity, delay, new BatchProcessor<RevisionInternal>() {
                @Override
                public void process(List<RevisionInternal> inbox) {
                    insertDownloads(inbox);
                }
            });
        }
        pendingSequences = new SequenceMap();
        Log.w(Database.TAG, this + ": starting ChangeTracker with since=" + lastSequence);
        changeTracker = new ChangeTracker(remote, continuous ? ChangeTracker.ChangeTrackerMode.LongPoll : ChangeTracker.ChangeTrackerMode.OneShot, true, lastSequence, this);
        Log.w(Database.TAG, this + ": started ChangeTracker " + changeTracker);

        if(filterName != null) {
            changeTracker.setFilterName(filterName);
            if(filterParams != null) {
                changeTracker.setFilterParams(filterParams);
            }
        }
        changeTracker.setDocIDs(documentIDs);
        changeTracker.setRequestHeaders(requestHeaders);
        if(!continuous) {
            Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": beginReplicating() calling asyncTaskStarted()");
            asyncTaskStarted();
        }
        changeTracker.setUsePOST(serverIsSyncGatewayVersion("0.93"));
        changeTracker.start();
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
        String docID = (String)change.get("id");
        if(docID == null) {
            return;
        }
        if(!Database.isValidDocumentId(docID)) {
            Log.w(Database.TAG, String.format("%s: Received invalid doc ID from _changes: %s", this, change));
            return;
        }
        boolean deleted = (change.containsKey("deleted") && ((Boolean)change.get("deleted")).equals(Boolean.TRUE));
        List<Map<String,Object>> changes = (List<Map<String,Object>>)change.get("changes");
        for (Map<String, Object> changeDict : changes) {
            String revID = (String)changeDict.get("rev");
            if(revID == null) {
                continue;
            }
            PulledRevision rev = new PulledRevision(docID, revID, deleted, db);
            rev.setRemoteSequenceID(lastSequence);
            Log.d(Database.TAG, this + ": adding rev to inbox " + rev);

            Log.v(Database.TAG, String.format("%s: changeTrackerReceivedChange() incrementing changesCount by 1", this));

            // this is purposefully done slightly different than the ios version
            setChangesCount(getChangesCount() + 1);

            addToInbox(rev);
        }

        while(revsToPull != null && revsToPull.size() > 1000) {
            try {
                Thread.sleep(500);  // <-- TODO: why is this here?
            } catch(InterruptedException e) {

            }
        }
    }

    @Override
    @InterfaceAudience.Private
    public void changeTrackerStopped(ChangeTracker tracker) {
        Log.w(Database.TAG, this + ": ChangeTracker " + tracker + " stopped");
        if (error == null && tracker.getLastError() != null) {
            setError(tracker.getLastError());
        }
        changeTracker = null;
        if(batcher != null) {
            Log.d(Database.TAG, this + ": calling batcher.flush().  batcher.count() is " + batcher.count());
            batcher.flush();
        }
        if (!isContinuous()) {
            Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": changeTrackerStopped() calling asyncTaskFinished()");

            // the asyncTaskFinished needs to run on the work executor
            // in order to fix https://github.com/couchbase/couchbase-lite-java-core/issues/91
            // basically, bad things happen when this runs on ChangeTracker thread.
            workExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    asyncTaskFinished(1);  // balances -asyncTaskStarted in -startChangeTracker
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
        // Ask the local database which of the revs are not known to it:
        String lastInboxSequence = ((PulledRevision)inbox.get(inbox.size()-1)).getRemoteSequenceID();

        int numRevisionsRemoved = 0;
        try {
             // findMissingRevisions is the local equivalent of _revs_diff. it looks at the
             // array of revisions in ‘inbox’ and removes the ones that already exist. So whatever’s left in ‘inbox’
             // afterwards are the revisions that need to be downloaded.
            numRevisionsRemoved = db.findMissingRevisions(inbox);
        } catch (SQLException e) {
            Log.e(Database.TAG, String.format("%s failed to look up local revs", this), e);
            inbox = null;
        }

        //introducing this to java version since inbox may now be null everywhere
        int inboxCount = 0;
        if(inbox != null) {
            inboxCount = inbox.size();
        }

        if(numRevisionsRemoved > 0) {
            Log.v(Database.TAG, String.format("%s: processInbox() setting changesCount to: %s", this, getChangesCount() - numRevisionsRemoved));
            // May decrease the changesCount, to account for the revisions we just found out we don’t need to get.
            setChangesCount(getChangesCount() - numRevisionsRemoved);
        }

        if(inboxCount == 0) {
            // Nothing to do. Just bump the lastSequence.
            Log.w(Database.TAG, String.format("%s no new remote revisions to fetch", this));
            long seq = pendingSequences.addValue(lastInboxSequence);
            pendingSequences.removeSequence(seq);
            setLastSequence(pendingSequences.getCheckpointedValue());
            return;
        }

        Log.v(Database.TAG, this + " fetching " + inboxCount + " remote revisions...");

        // Dump the revs into the queue of revs to pull from the remote db:
        synchronized (this) {
	        if(revsToPull == null) {
	            revsToPull = new ArrayList<RevisionInternal>(200);
	        }

	        for(int i=0; i < inbox.size(); i++) {
	            PulledRevision rev = (PulledRevision)inbox.get(i);
				// FIXME add logic here to pull initial revs in bulk
	            rev.setSequence(pendingSequences.addValue(rev.getRemoteSequenceID()));
	            revsToPull.add(rev);
	        }
		}

        pullRemoteRevisions();
    }

    /**
     * Start up some HTTP GETs, within our limit on the maximum simultaneous number
     *
     * The entire method is not synchronized, only the portion pulling work off the list
     * Important to not hold the synchronized block while we do network access
     */
    @InterfaceAudience.Private
    public void pullRemoteRevisions() {
        Log.d(Database.TAG, this + ": pullRemoteRevisions() with revsToPull size: " + revsToPull.size());
        //find the work to be done in a synchronized block
        List<RevisionInternal> workToStartNow = new ArrayList<RevisionInternal>();
        synchronized (this) {
			while(httpConnectionCount + workToStartNow.size() < MAX_OPEN_HTTP_CONNECTIONS && revsToPull != null && revsToPull.size() > 0) {
				RevisionInternal work = revsToPull.remove(0);
                Log.d(Database.TAG, this + ": add " + work + " to workToStartNow");
                workToStartNow.add(work);
			}
		}

        //actually run it outside the synchronized block
        for(RevisionInternal work : workToStartNow) {
            pullRemoteRevision(work);
        }
    }

    /**
     * Fetches the contents of a revision from the remote db, including its parent revision ID.
     * The contents are stored into rev.properties.
     */
    @InterfaceAudience.Private
    public void pullRemoteRevision(final RevisionInternal rev) {

        Log.d(Database.TAG, this + "|" + Thread.currentThread().toString() + ": pullRemoteRevision with rev: " + rev);

        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": pullRemoteRevision() calling asyncTaskStarted()");

        asyncTaskStarted();
        ++httpConnectionCount;

        // Construct a query. We want the revision history, and the bodies of attachments that have
        // been added since the latest revisions we have locally.
        // See: http://wiki.apache.org/couchdb/HTTP_Document_API#Getting_Attachments_With_a_Document
        StringBuilder path = new StringBuilder("/" + URLEncoder.encode(rev.getDocId()) + "?rev=" + URLEncoder.encode(rev.getRevId()) + "&revs=true&attachments=true");
        List<String> knownRevs = knownCurrentRevIDs(rev);
        if(knownRevs == null) {
            //this means something is wrong, possibly the replicator has shut down
            Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": pullRemoteRevision() calling asyncTaskFinished()");
            asyncTaskFinished(1);
            --httpConnectionCount;
            return;
        }
        if(knownRevs.size() > 0) {
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
                        Log.e(Database.TAG, "Error pulling remote revision", e);
                        setError(e);
                        revisionFailed();
                        Log.d(Database.TAG, this + " pullRemoteRevision() updating completedChangesCount from " + getCompletedChangesCount() + " -> " + getCompletedChangesCount() + 1 + " due to error pulling remote revision");
                        setCompletedChangesCount(getCompletedChangesCount() + 1);
                    } else {
                        Map<String,Object> properties = (Map<String,Object>)result;
                        PulledRevision gotRev = new PulledRevision(properties, db);
                        gotRev.setSequence(rev.getSequence());
                        // Add to batcher ... eventually it will be fed to -insertDownloads:.
                        Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": pullRemoteRevision() calling asyncTaskStarted() - innner");

                        asyncTaskStarted();
                        // TODO: [gotRev.body compact];
                        Log.d(Database.TAG, this + ": pullRemoteRevision add rev: " + gotRev + " to batcher");
                        downloadsToInsert.queueObject(gotRev);

                    }

                } finally {
                    Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": pullRemoteRevision.onCompletion() calling asyncTaskFinished()");
                    asyncTaskFinished(1);
                }

                // Note that we've finished this task; then start another one if there
                // are still revisions waiting to be pulled:
                --httpConnectionCount;
                pullRemoteRevisions();

            }
        });

    }

    /**
     * This will be called when _revsToInsert fills up:
     */
    @InterfaceAudience.Private
    public void insertDownloads(List<RevisionInternal> downloads) {

        Log.i(Database.TAG, this + " inserting " + downloads.size() + " revisions...");
        long time = System.currentTimeMillis();
        Collections.sort(downloads, getRevisionListComparator());

        db.beginTransaction();
        boolean success = false;
        try {
            for (RevisionInternal rev : downloads) {
                long fakeSequence = rev.getSequence();
                List<String> history = db.parseCouchDBRevisionHistory(rev.getProperties());
                if (history.isEmpty() && rev.getGeneration() > 1) {
                    Log.w(Database.TAG, this + ": Missing revision history in response for: " + rev);
                    setError(new CouchbaseLiteException(Status.UPSTREAM_ERROR));
                    revisionFailed();
                    continue;
                }

                Log.v(Database.TAG, this + " inserting " + rev.getDocId() + " " + history);

                // Insert the revision
                try {
                    db.forceInsert(rev, history, remote);
                } catch (CouchbaseLiteException e) {
                    if (e.getCBLStatus().getCode() == Status.FORBIDDEN) {
                        Log.i(Database.TAG, this + ": Remote rev failed validation: " + rev);
                    } else {
                        Log.w(Database.TAG, this + " failed to write " + rev + ": status=" + e.getCBLStatus().getCode());
                        revisionFailed();
                        setError(new HttpResponseException(e.getCBLStatus().getCode(), null));
                        continue;
                    }
                }

                // Mark this revision's fake sequence as processed:
                pendingSequences.removeSequence(fakeSequence);

            }

            Log.v(Database.TAG, this + " finished inserting " + downloads.size() + "revisions");
            success = true;

        } catch(SQLException e) {
            Log.e(Database.TAG, this + ": Exception inserting revisions", e);
        } finally {
            db.endTransaction(success);
            Log.d(Database.TAG, this + "|" + Thread.currentThread() + ": insertDownloads() calling asyncTaskFinished() with value: " + downloads.size());
            asyncTaskFinished(downloads.size());
        }

        // Checkpoint:
        setLastSequence(pendingSequences.getCheckpointedValue());

        long delta = System.currentTimeMillis() - time;
        Log.v(Database.TAG, this + " inserted " + downloads.size() + " revs in " + delta + " milliseconds");

        Log.d(Database.TAG, this + " insertDownloads() updating completedChangesCount from " + getCompletedChangesCount() + " -> " + getCompletedChangesCount() + downloads.size());

        setCompletedChangesCount(getCompletedChangesCount() + downloads.size());


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
    /* package */ List<String> knownCurrentRevIDs(RevisionInternal rev) {
        if(db != null) {
            return db.getAllRevisionsOfDocumentID(rev.getDocId(), true).getAllRevIds();
        }
        return null;
    }

    @InterfaceAudience.Private
    public String joinQuotedEscaped(List<String> strings) {
        if(strings.size() == 0) {
            return "[]";
        }
        byte[] json = null;
        try {
            json = Manager.getObjectMapper().writeValueAsBytes(strings);
        } catch (Exception e) {
            Log.w(Database.TAG, "Unable to serialize json", e);
        }
        return URLEncoder.encode(new String(json));
    }


    @InterfaceAudience.Public
    public boolean goOffline() {
        Log.d(Database.TAG, this + " goOffline() called, stopping changeTracker: " + changeTracker);
        if (!super.goOffline()) {
            return false;
        }

        if (changeTracker != null) {
            changeTracker.stop();
        }

        return true;
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
