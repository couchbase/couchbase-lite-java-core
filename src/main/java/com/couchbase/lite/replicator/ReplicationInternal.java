package com.couchbase.lite.replicator;

import com.couchbase.lite.Database;
import com.couchbase.lite.Misc;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.auth.AuthenticatorImpl;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.BatchProcessor;
import com.couchbase.lite.support.Batcher;
import com.couchbase.lite.support.BlockingQueueListener;
import com.couchbase.lite.support.CustomFuture;
import com.couchbase.lite.support.CustomLinkedBlockingQueue;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.support.RemoteRequestRetry;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.TextUtils;
import com.couchbase.lite.util.URIUtils;
import com.couchbase.lite.util.Utils;
import com.couchbase.org.apache.http.entity.mime.MultipartEntity;
import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.delegates.Action1;
import com.github.oxo42.stateless4j.transitions.Transition;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.cookie.BasicClientCookie2;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Internal Replication object that does the heavy lifting
 *
 * @exclude
 */
@InterfaceAudience.Private
abstract class ReplicationInternal implements BlockingQueueListener {

    // Change listeners can be called back synchronously or asynchronously.
    protected enum ChangeListenerNotifyStyle {
        SYNC, ASYNC
    }

    public static final String BY_CHANNEL_FILTER_NAME = "sync_gateway/bychannel";

    public static final String CHANNELS_QUERY_PARAM = "channels";

    public static final int EXECUTOR_THREAD_POOL_SIZE = 5;

    private static int lastSessionID = 0;

    public static int MAX_RETRIES = 10;  // total number of attempts = 11 (1 initial + MAX_RETRIES)
    public static int RETRY_DELAY_SECONDS = 60; // #define kRetryDelay 60.0 in CBL_Replicator.m

    protected Replication parentReplication;
    protected Database db;
    protected URL remote;
    protected HttpClientFactory clientFactory;
    protected String lastSequence;
    protected Authenticator authenticator;
    protected String filterName;
    protected Map<String, Object> filterParams;
    protected List<String> documentIDs;
    protected Map<String, Object> requestHeaders;
    private String serverType;
    protected Batcher<RevisionInternal> batcher;
    protected static final int PROCESSOR_DELAY = 500;
    protected static int INBOX_CAPACITY = 100;
    protected ScheduledExecutorService remoteRequestExecutor;
    protected Throwable error;
    private String remoteCheckpointDocID;
    protected Map<String, Object> remoteCheckpoint;
    protected AtomicInteger completedChangesCount;
    protected AtomicInteger changesCount;
    protected CollectionUtils.Functor<RevisionInternal, RevisionInternal> revisionBodyTransformationBlock;
    protected String sessionID;
    protected BlockingQueue<Future> pendingFutures;
    private boolean lastSequenceChanged = false;
    private boolean savingCheckpoint;
    private boolean overdueForCheckpointSave;


    // the code assumes this is a _single threaded_ work executor.
    // if it's not, the behavior will be buggy.  I don't see a way to assert this in the code.
    protected ScheduledExecutorService workExecutor;

    protected StateMachine<ReplicationState, ReplicationTrigger> stateMachine;
    protected List<ChangeListener> changeListeners;
    protected Replication.Lifecycle lifecycle;
    protected ChangeListenerNotifyStyle changeListenerNotifyStyle;

    private Future retryFuture = null;  // future obj of retry task
    private int retryCount = 0;         // counter for retry.

    /**
     * Constructor
     */
    ReplicationInternal(Database db, URL remote,
                        HttpClientFactory clientFactory,
                        ScheduledExecutorService workExecutor,
                        Replication.Lifecycle lifecycle,
                        Replication parentReplication) {

        Utils.assertNotNull(lifecycle, "Must pass in a non-null lifecycle");

        this.parentReplication = parentReplication;
        this.db = db;
        this.remote = remote;
        this.clientFactory = clientFactory;
        this.workExecutor = workExecutor;
        this.lifecycle = lifecycle;

        this.requestHeaders = new HashMap<String, Object>();

        changeListeners = new CopyOnWriteArrayList<ChangeListener>();

        // The reason that notifications are ASYNC is to make the public API call
        // Replication.getStatus() work as expected.  Because if this is set to SYNC,
        // it causes the following issue:
        // - Notification sent from state transition from INITIAL -> RUNNING.
        // - Replication change listener called back during transition.
        // - Replication change listener calls replication.status(), and gets INITIAL instead of RUNNING.
        // - Replication change listener never notified when it goes into the RUNNING state.
        // Workarounds to above problem:
        // - By sending notifications ASYNC, by the time that the listener calls replication.status(),
        //   calling replication.getStatus() will return RUNNING.
        // - Alternatively, change listeners could look at the passed transition rather than
        //   depending on calling replication.status(), and changeListenerNotifyStyle could be set to SYNC.
        changeListenerNotifyStyle = ChangeListenerNotifyStyle.ASYNC;

        pendingFutures = new CustomLinkedBlockingQueue<Future>(this);

        initializeStateMachine();

        this.retryCount = 0;
    }

    /**
     * Trigger this replication to start (async)
     */
    public void triggerStart() {
        fireTrigger(ReplicationTrigger.START);
    }

    /**
     * Trigger this replication to stop (async)
     */
    public void triggerStop() {
        fireTrigger(ReplicationTrigger.STOP_GRACEFUL);
    }

    /**
     * Trigger this replication to go offline (async)
     */
    public void triggerGoOffline() {
        fireTrigger(ReplicationTrigger.GO_OFFLINE);
    }

    /**
     * Trigger this replication to go online (async)
     */
    public void triggerGoOnline() {
        fireTrigger(ReplicationTrigger.GO_ONLINE);
    }

    /**
     * Fire a trigger to the state machine
     */
    protected void fireTrigger(final ReplicationTrigger trigger) {
        Log.d(Log.TAG_SYNC, "[fireTrigger()] => " + trigger);
        // All state machine triggers need to happen on the replicator thread
        synchronized (workExecutor) {
            if (!workExecutor.isShutdown()) {
                workExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Log.d(Log.TAG_SYNC, "firing trigger: %s", trigger);
                            stateMachine.fire(trigger);
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        }
    }

    /**
     * Trigger this replication to stop immediately -- assumes pending work has
     * been drained, or that caller chooses to ignore any pending work.
     */
    protected void triggerStopImmediate() {
        fireTrigger(ReplicationTrigger.STOP_IMMEDIATE);
    }

    /**
     * Start the replication process.
     */
    protected void start() {

        try {
            if (!db.isOpen()) {

                String msg = String.format("Db: %s is not open, abort replication", db);
                parentReplication.setLastError(new Exception(msg));

                fireTrigger(ReplicationTrigger.STOP_IMMEDIATE);

                return;

            }

            db.addReplication(parentReplication);
            db.addActiveReplication(parentReplication);

            initSessionId();

            // init batcher
            initBatcher();

            // init authorizer / authenticator
            initAuthorizer();

            // call goOnline (or trigger state change into online state)
            goOnlineInitialStartup();

            initNetworkReachabilityManager();

            this.retryCount = 0;
        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, "%s: Exception in start()", e, this);
        }

    }

    private void initSessionId() {
        this.sessionID = String.format("repl%03d", ++lastSessionID);
    }

    /**
     * Take the replication offline
     */
    protected void goOffline() {
        // implemented by subclasses
    }

    /**
     * Put the replication back online after being offline
     */
    protected void goOnline() {
        // implemented by subclasses

    }

    public void databaseClosing() {
        triggerStop();
    }

    /**
     * Close all resources associated with this replicator.
     */
    protected void close() {
        // shutdown ScheduledExecutorService. Without shutdown, cause thread leak
        if (remoteRequestExecutor != null && !remoteRequestExecutor.isShutdown()) {
            // Note: Time to wait is set 60 sec because RemoteRequest's socket timeout is set 60 seconds.
            Utils.shutdownAndAwaitTermination(remoteRequestExecutor, Replication.DEFAULT_MAX_TIMEOUT_FOR_SHUTDOWN);
        }
    }

    protected void initAuthorizer() {
        // TODO: add this back in  .. See Replication constructor

    }

    protected void initBatcher() {

        batcher = new Batcher<RevisionInternal>(workExecutor, INBOX_CAPACITY, PROCESSOR_DELAY, new BatchProcessor<RevisionInternal>() {
            @Override
            public void process(List<RevisionInternal> inbox) {

                try {
                    Log.v(Log.TAG_SYNC, "*** %s: BEGIN processInbox (%d sequences)", this, inbox.size());
                    processInbox(new RevisionList(inbox));
                    Log.v(Log.TAG_SYNC, "*** %s: END processInbox (lastSequence=%s)", this, lastSequence);
                } catch (Exception e) {
                    Log.e(Log.TAG_SYNC, "ERROR: processInbox failed: ", e);
                    throw new RuntimeException(e);
                }
            }
        });


    }

    protected void initNetworkReachabilityManager() {
        db.getManager().getContext().getNetworkReachabilityManager().addNetworkReachabilityListener(parentReplication);
    }

    public abstract boolean shouldCreateTarget();

    public abstract void setCreateTarget(boolean createTarget);

    protected void goOnlineInitialStartup() {

        int executorThreadPoolSize = db.getManager().getExecutorThreadPoolSize() <= 0 ?
                EXECUTOR_THREAD_POOL_SIZE : db.getManager().getExecutorThreadPoolSize();
        Log.v(Log.TAG_SYNC, "executorThreadPoolSize=" + executorThreadPoolSize);
        remoteRequestExecutor = Executors.newScheduledThreadPool(executorThreadPoolSize, new ThreadFactory() {
            private int counter = 0;

            @Override
            public Thread newThread(Runnable r) {
                String threadName = "CBLRequestWorker";
                try {
                    String replicationIdentifier = Utils.shortenString(remoteCheckpointDocID(), 5);
                    threadName = String.format("CBLRequestWorker-%s-%s", replicationIdentifier, counter++);
                } catch (Exception e) {
                    Log.e(Log.TAG_SYNC, "Error creating thread name", e);
                }
                return new Thread(r, threadName);
            }
        });

        checkSession();

    }

    @InterfaceAudience.Private
    protected void checkSession() {
        // REVIEW : This is not in line with the iOS implementation
        if (getAuthenticator() != null && ((AuthenticatorImpl) getAuthenticator()).usesCookieBasedLogin()) {
            checkSessionAtPath("/_session");
        } else {
            fetchRemoteCheckpointDoc();
        }
    }

    @InterfaceAudience.Private
    protected void checkSessionAtPath(final String sessionPath) {

        Future future = sendAsyncRequest("GET", sessionPath, null, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable error) {

                try {
                    if (error != null) {
                        // If not at /db/_session, try CouchDB location /_session
                        if (error instanceof HttpResponseException &&
                                ((HttpResponseException) error).getStatusCode() == 404 &&
                                sessionPath.equalsIgnoreCase("/_session")) {

                            checkSessionAtPath("_session");
                            return;
                        }
                        Log.e(Log.TAG_SYNC, this + ": Session check failed", error);
                        setError(error);

                    } else {
                        Map<String, Object> response = (Map<String, Object>) result;
                        Log.e(Log.TAG_SYNC, "%s checkSessionAtPath() response: %s", this, response);
                        Map<String, Object> userCtx = (Map<String, Object>) response.get("userCtx");
                        String username = (String) userCtx.get("name");
                        if (username != null && username.length() > 0) {
                            Log.d(Log.TAG_SYNC, "%s Active session, logged in as %s", this, username);
                            fetchRemoteCheckpointDoc();
                        } else {
                            Log.d(Log.TAG_SYNC, "%s No active session, going to login", this);
                            login();
                        }
                    }

                } catch (Exception e) {
                    Log.e(Log.TAG_SYNC, "%s Exception in checkSessionAtPath()", this, e);
                }

            }

        });
        pendingFutures.add(future);
    }

    @InterfaceAudience.Private
    protected void login() {
        Map<String, String> loginParameters = ((AuthenticatorImpl) getAuthenticator()).loginParametersForSite(remote);
        if (loginParameters == null) {
            Log.d(Log.TAG_SYNC, "%s: %s has no login parameters, so skipping login", this, getAuthenticator());
            fetchRemoteCheckpointDoc();
            return;
        }

        final String loginPath = ((AuthenticatorImpl) getAuthenticator()).loginPathForSite(remote);

        Log.d(Log.TAG_SYNC, "%s: Doing login with %s at %s", this, getAuthenticator().getClass(), loginPath);

        Future future = sendAsyncRequest("POST", loginPath, loginParameters, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                if (e != null) {
                    Log.d(Log.TAG_SYNC, "%s: Login failed for path: %s", this, loginPath);
                    setError(e);

                    // TODO: double check this behavior against iOS implementation, especially
                    // TODO: with regards to behavior of a continuous replication.
                    // Note: was added in order that unit test testReplicatorErrorStatus() finished and passed.
                    // (before adding this, the replication would just end up in limbo and never finish)
                    triggerStop();

                } else {
                    Log.v(Log.TAG_SYNC, "%s: Successfully logged in!", this);
                    fetchRemoteCheckpointDoc();
                }
            }

        });
        pendingFutures.add(future);

    }

    @InterfaceAudience.Private
    protected void setError(Throwable throwable) {

        // TODO
        /*
        if (error.code == NSURLErrorCancelled && $equal(error.domain, NSURLErrorDomain))
            return;
         */

        if (throwable != error) {
            Log.e(Log.TAG_SYNC, "%s: Progress: set error = %s", this, throwable);
            parentReplication.setLastError(throwable);
            error = throwable;
            Replication.ChangeEvent changeEvent = new Replication.ChangeEvent(this);
            changeEvent.setError(error);
            notifyChangeListeners(changeEvent);
        }

        // #352
        // iOS version: stop replicator immediately when call setError() with permanent error.
        // But, for Core Java, some of codes wait IDLE state. So this is reason to wait till
        // state becomes IDLE.
    }


    @InterfaceAudience.Private
    protected void addToCompletedChangesCount(int delta) {
        int previousVal = getCompletedChangesCount().getAndAdd(delta);
        Log.v(Log.TAG_SYNC, "%s: Incrementing completedChangesCount count from %s by adding %d -> %d",
                this, previousVal, delta, completedChangesCount.get());
        Replication.ChangeEvent changeEvent = new Replication.ChangeEvent(this);
        notifyChangeListeners(changeEvent);
    }

    @InterfaceAudience.Private
    protected void addToChangesCount(int delta) {
        int previousVal = getChangesCount().getAndAdd(delta);
        if (getChangesCount().get() < 0) {
            Log.w(Log.TAG_SYNC, "Changes count is negative, this could indicate an error");
        }
        Log.v(Log.TAG_SYNC, "%s: Incrementing changesCount count from %s by adding %d -> %d",
                this, previousVal, delta, changesCount.get());
        Replication.ChangeEvent changeEvent = new Replication.ChangeEvent(this);
        notifyChangeListeners(changeEvent);
    }

    public AtomicInteger getCompletedChangesCount() {
        if (completedChangesCount == null) {
            completedChangesCount = new AtomicInteger(0);
        }
        return completedChangesCount;
    }

    public AtomicInteger getChangesCount() {
        if (changesCount == null) {
            changesCount = new AtomicInteger(0);
        }
        return changesCount;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public CustomFuture sendAsyncRequest(String method, String relativePath, Object body,
                                   RemoteRequestCompletionBlock onCompletion) {
        return sendAsyncRequest(method, relativePath, body, false, onCompletion);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public CustomFuture sendAsyncRequest(String method, String relativePath, Object body, boolean dontLog404,
                                   RemoteRequestCompletionBlock onCompletion) {
        try {
            String urlStr = buildRelativeURLString(relativePath);
            URL url = new URL(urlStr);
            return sendAsyncRequest(method, url, body, dontLog404, onCompletion);
        } catch (MalformedURLException e) {
            Log.e(Log.TAG_SYNC, "Malformed URL for async request", e);
        }
        return null;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public CustomFuture sendAsyncRequest(String method, URL url, Object body, boolean dontLog404,
                                   final RemoteRequestCompletionBlock onCompletion) {
        Log.d(Log.TAG_SYNC, "[sendAsyncRequest()] " + method + " => " + url);
        RemoteRequestRetry request = new RemoteRequestRetry(
                RemoteRequestRetry.RemoteRequestType.REMOTE_REQUEST,
                remoteRequestExecutor,
                workExecutor,
                clientFactory,
                method,
                url,
                body,
                getLocalDatabase(),
                getHeaders(),
                onCompletion
        );

        request.setDontLog404(dontLog404);

        request.setAuthenticator(getAuthenticator());
        request.setOnPreCompletionCaller(new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                if (serverType == null && httpResponse != null) {
                    Header serverHeader = httpResponse.getFirstHeader("Server");
                    if (serverHeader != null) {
                        String serverVersion = serverHeader.getValue();
                        Log.v(Log.TAG_SYNC, "serverVersion: %s", serverVersion);
                        serverType = serverVersion;
                    }
                }
            }
        });


        return request.submit(canSendCompressedRequests());
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public CustomFuture sendAsyncMultipartRequest(String method, String relativePath,
                                            MultipartEntity multiPartEntity,
                                            RemoteRequestCompletionBlock onCompletion) {
        URL url = null;
        try {
            String urlStr = buildRelativeURLString(relativePath);
            url = new URL(urlStr);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }

        RemoteRequestRetry request = new RemoteRequestRetry(
                RemoteRequestRetry.RemoteRequestType.REMOTE_MULTIPART_REQUEST,
                remoteRequestExecutor,
                workExecutor,
                clientFactory,
                method,
                url,
                multiPartEntity,
                getLocalDatabase(),
                getHeaders(),
                onCompletion
        );

        request.setAuthenticator(getAuthenticator());

        return request.submit();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public CustomFuture sendAsyncMultipartDownloaderRequest(String method, String relativePath,
                                                            Object body, Database db,
                                                            RemoteRequestCompletionBlock onCompletion) {
        try {

            String urlStr = buildRelativeURLString(relativePath);
            URL url = new URL(urlStr);

            RemoteRequestRetry request = new RemoteRequestRetry(
                    RemoteRequestRetry.RemoteRequestType.REMOTE_MULTIPART_DOWNLOADER_REQUEST,
                    remoteRequestExecutor,
                    workExecutor,
                    clientFactory,
                    method,
                    url,
                    body,
                    getLocalDatabase(),
                    getHeaders(),
                    onCompletion
            );

            request.setAuthenticator(getAuthenticator());

            return request.submit();
        } catch (MalformedURLException e) {
            Log.e(Log.TAG_SYNC, "Malformed URL for async request", e);
        }
        return null;
    }


    /**
     * Get the local database which is the source or target of this replication
     */
    protected Database getLocalDatabase() {
        return db;
    }

    protected void setLocalDatabase(Database db) {
        this.db = db;
    }

    /**
     * Extra HTTP headers to send in all requests to the remote server.
     * Should map strings (header names) to strings.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getHeaders() {
        return requestHeaders;
    }

    /**
     * Set Extra HTTP headers to be sent in all requests to the remote server.
     */
    @InterfaceAudience.Public
    public void setHeaders(Map<String, Object> requestHeadersParam) {
        if (requestHeadersParam != null && !requestHeaders.equals(requestHeadersParam)) {
            requestHeaders = requestHeadersParam;
        }
    }

    /**
     * in CBL_Replicator.m
     * - (void) saveLastSequence
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public void saveLastSequence() {
        if (!lastSequenceChanged) {
            return;
        }

        if (savingCheckpoint) {
            // If a save is already in progress, don't do anything. (The completion block will trigger
            // another save after the first one finishes.)
            overdueForCheckpointSave = true;
            return;
        }

        lastSequenceChanged = false;
        overdueForCheckpointSave = false;

        Log.d(Log.TAG_SYNC, "%s: saveLastSequence() called. lastSequence: %s remoteCheckpoint: %s",
                this, lastSequence, remoteCheckpoint);
        final Map<String, Object> body = new HashMap<String, Object>();
        if (remoteCheckpoint != null) {
            body.putAll(remoteCheckpoint);
        }
        body.put("lastSequence", lastSequence);

        savingCheckpoint = true;
        final String remoteCheckpointDocID = remoteCheckpointDocID();
        if (remoteCheckpointDocID == null) {
            Log.w(Log.TAG_SYNC, "%s: remoteCheckpointDocID is null, aborting saveLastSequence()", this);
            return;
        }

        final String checkpointID = remoteCheckpointDocID;
        Log.d(Log.TAG_SYNC, "%s: start put remote _local document.  checkpointID: %s body: %s",
                this, checkpointID, body);
        Future future = sendAsyncRequest("PUT", "/_local/" + checkpointID, body, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {

                Log.d(Log.TAG_SYNC,
                        "%s: put remote _local document request finished.  checkpointID: %s body: %s",
                        this, checkpointID, body);

                try {

                    if (e != null) {
                        Log.w(Log.TAG_SYNC, "%s: Unable to save remote checkpoint", e, this);
                        // Failed to save checkpoint:
                        switch (Utils.getStatusFromError(e)) {
                            case Status.NOT_FOUND:
                                Log.i(Log.TAG_SYNC, "%s: could not save remote checkpoint: 404 NOT FOUND", this);
                                remoteCheckpoint = null;  // doc deleted or db reset
                                overdueForCheckpointSave = true; // try saving again
                                break;
                            case Status.CONFLICT:
                                Log.i(Log.TAG_SYNC, "%s: could not save remote checkpoint: 409 CONFLICT", this);
                                refreshRemoteCheckpointDoc();
                                break;
                            default:
                                Log.i(Log.TAG_SYNC, "%s: could not save remote checkpoint: %s", this, e);
                                // TODO: On 401 or 403, and this is a pull, remember that remote
                                // TODo: is read-only & don't attempt to read its checkpoint next time.
                                break;
                        }
                    } else {
                        // Saved checkpoint:
                        Map<String, Object> response = (Map<String, Object>) result;
                        body.put("_rev", response.get("rev"));
                        remoteCheckpoint = body;
                        if (db != null && db.open()) {
                            Log.d(Log.TAG_SYNC,
                                    "%s: saved remote checkpoint, updating local checkpoint.  remoteCheckpoint: %s",
                                    this, remoteCheckpoint);
                            db.setLastSequence(lastSequence, checkpointID);
                        } else {
                            Log.w(Log.TAG_SYNC, "%s: Database is null or closed, not calling db.setLastSequence() ", this);
                        }
                    }
                } finally {

                    savingCheckpoint = false;

                    if (overdueForCheckpointSave) {
                        Log.i(Log.TAG_SYNC, "%s: overdueForCheckpointSave == true, calling saveLastSequence()", this);
                        overdueForCheckpointSave = false;
                        saveLastSequence();
                    }
                }
            }
        });
        pendingFutures.add(future);
    }

    /**
     * Variant of -fetchRemoveCheckpointDoc that's used while replication is running, to reload the
     * checkpoint to get its current revision number, if there was an error saving it.
     */
    @InterfaceAudience.Private
    private void refreshRemoteCheckpointDoc() {
        Log.i(Log.TAG_SYNC, "%s: Refreshing remote checkpoint to get its _rev...", this);
        Future future = sendAsyncRequest("GET", "/_local/" + remoteCheckpointDocID(), null, new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                if (db == null) {
                    Log.w(Log.TAG_SYNC, "%s: db == null while refreshing remote checkpoint.  aborting", this);
                    return;
                }
                if (e != null && Utils.getStatusFromError(e) != Status.NOT_FOUND) {
                    Log.e(Log.TAG_SYNC, "%s: Error refreshing remote checkpoint", e, this);
                } else {
                    Log.d(Log.TAG_SYNC, "%s: Refreshed remote checkpoint: %s", this, result);
                    remoteCheckpoint = (Map<String, Object>) result;
                    lastSequenceChanged = true;
                    saveLastSequence();  // try saving again
                }
            }
        });
        pendingFutures.add(future);

    }

    @InterfaceAudience.Private
    protected String buildRelativeURLString(String relativePath) {

        // the following code is a band-aid for a system problem in the codebase
        // where it is appending "relative paths" that start with a slash, eg:
        //     http://dotcom/db/ + /relpart == http://dotcom/db/relpart
        // which is not compatible with the way the java url concatonation works.

        String remoteUrlString = remote.toExternalForm();
        if (remoteUrlString.endsWith("/") && relativePath.startsWith("/")) {
            remoteUrlString = remoteUrlString.substring(0, remoteUrlString.length() - 1);
        }

        // workaround for https://github.com/couchbase/couchbase-lite-java-core/issues/208
        if (relativePath.equals("_session")) {
            try {
                URL remoteUrl = new URL(remoteUrlString);
                String relativePathWithLeadingSlash = String.format("/%s", relativePath);  // required on couchbase-lite-java
                URL remoteUrlNoPath = new URL(remoteUrl.getProtocol(), remoteUrl.getHost(),
                        remoteUrl.getPort(), relativePathWithLeadingSlash);
                remoteUrlString = remoteUrlNoPath.toExternalForm();
                return remoteUrlString;
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        return remoteUrlString + relativePath;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void fetchRemoteCheckpointDoc() {
        lastSequenceChanged = false;
        String checkpointId = remoteCheckpointDocID();
        final String localLastSequence = db.lastSequenceWithCheckpointId(checkpointId);
        boolean dontLog404 = true;
        Future future = sendAsyncRequest("GET", "/_local/" + checkpointId, null, dontLog404, new RemoteRequestCompletionBlock() {

            @Override
            public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
                if (e != null && !Utils.is404(e)) {
                    Log.w(Log.TAG_SYNC, "%s: error getting remote checkpoint", e, this);
                    setError(e);

                    // TODO: double check this behavior against iOS implementation, especially
                    // TODO: with regards to behavior of a continuous replication.
                    // Note: was added in order that unit test testRunReplicationWithError() finished and passed.
                    // (before adding this, the replication would just end up in limbo and never finish)
                    fireTrigger(ReplicationTrigger.STOP_GRACEFUL);  // TODO: call triggerStop(); just to be more consistent

                } else {
                    if (e != null && Utils.is404(e)) {
                        Log.v(Log.TAG_SYNC, "%s: Remote checkpoint does not exist on server yet: %s",
                                this, remoteCheckpointDocID());
                        maybeCreateRemoteDB();
                    }
                    Map<String, Object> response = (Map<String, Object>) result;
                    remoteCheckpoint = response;
                    String remoteLastSequence = null;
                    if (response != null) {
                        remoteLastSequence = (String) response.get("lastSequence");
                    }
                    if (remoteLastSequence != null && remoteLastSequence.equals(localLastSequence)) {
                        lastSequence = localLastSequence;
                        Log.d(Log.TAG_SYNC, "%s: Replicating from lastSequence=%s", this, lastSequence);
                    } else {
                        Log.d(Log.TAG_SYNC, "%s: lastSequence mismatch: I had: %s, remote had: %s",
                                this, localLastSequence, remoteLastSequence);
                    }
                    beginReplicating();
                }
            }

        });
        pendingFutures.add(future);
    }


    protected abstract void maybeCreateRemoteDB();

    /**
     * This is the _local document ID stored on the remote server to keep track of state.
     * Its ID is based on the local database ID (the private one, to make the result unguessable)
     * and the remote database's URL.
     *
     * @exclude
     */

    public String remoteCheckpointDocID() {

        if (remoteCheckpointDocID != null) {
            return remoteCheckpointDocID;
        } else {

            // TODO: Needs to be consistent with -hasSameSettingsAs: --
            // TODO: If a.remoteCheckpointID == b.remoteCheckpointID then [a hasSameSettingsAs: b]

            if (db == null) {
                return null;
            }

            // canonicalization: make sure it produces the same checkpoint id regardless of
            // ordering of filterparams / docids
            Map<String, Object> filterParamsCanonical = null;
            if (getFilterParams() != null) {
                filterParamsCanonical = new TreeMap<String, Object>(getFilterParams());
            }

            List<String> docIdsSorted = null;
            if (getDocIds() != null) {
                docIdsSorted = new ArrayList<String>(getDocIds());
                Collections.sort(docIdsSorted);
            }

            // use a treemap rather than a dictionary for purposes of canonicalization
            Map<String, Object> spec = new TreeMap<String, Object>();
            spec.put("localUUID", db.privateUUID());
            spec.put("remoteURL", remote.toExternalForm());
            spec.put("push", !isPull());
            spec.put("continuous", isContinuous());
            if (getFilter() != null) {
                spec.put("filter", getFilter());
            }
            if (filterParamsCanonical != null) {
                spec.put("filterParams", filterParamsCanonical);
            }
            if (docIdsSorted != null) {
                spec.put("docids", docIdsSorted);
            }

            byte[] inputBytes = null;
            try {
                inputBytes = db.getManager().getObjectMapper().writeValueAsBytes(spec);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            remoteCheckpointDocID = Misc.HexSHA1Digest(inputBytes);
            return remoteCheckpointDocID;

        }

    }

    /**
     * For javadocs, see Replication
     */
    public String getFilter() {
        return filterName;
    }

    /**
     * Set the filter to be used by this replication
     */

    public void setFilter(String filterName) {
        this.filterName = filterName;
    }


    /**
     * Is this a pull replication?  (Eg, it pulls data from Sync Gateway -> Device running CBL?)
     */
    public abstract boolean isPull();

    /**
     * Gets the documents to specify as part of the replication.
     */
    public List<String> getDocIds() {
        return documentIDs;
    }

    /**
     * Sets the documents to specify as part of the replication.
     */
    public void setDocIds(List<String> docIds) {
        documentIDs = docIds;
    }

    /**
     * Should the replication operate continuously, copying changes as soon as the
     * source database is modified? (Defaults to NO).
     */
    public boolean isContinuous() {
        return lifecycle == Replication.Lifecycle.CONTINUOUS;
    }

    /**
     * For javadoc, see Replication
     */
    public Map<String, Object> getFilterParams() {
        return filterParams;
    }

    /**
     * Set parameters to pass to the filter function.
     */
    public void setFilterParams(Map<String, Object> filterParams) {
        this.filterParams = filterParams;
    }


    abstract protected void processInbox(RevisionList inbox);

    /**
     * gzip
     * <p/>
     * in CBL_Replicator.m
     * - (BOOL) canSendCompressedRequests
     */
    public boolean canSendCompressedRequests() {
        // https://github.com/couchbase/couchbase-lite-ios/issues/240#issuecomment-32506552
        // gzip upload is only enabled when the server is Sync Gateway 0.92 or later
        return serverIsSyncGatewayVersion("0.92");
    }

    /**
     * After successfully authenticating and getting remote checkpoint,
     * begin the work of transferring documents.
     */
    abstract protected void beginReplicating();

    /**
     * Actual work of stopping the replication process.
     */
    protected void stopGraceful() {
        Log.d(Log.TAG_SYNC, "stopGraceful()");
        this.retryCount = 0;
    }

    /**
     * Notify all change listeners of a ChangeEvent
     */
    private void notifyChangeListeners(final Replication.ChangeEvent changeEvent) {
        if (changeListenerNotifyStyle == ChangeListenerNotifyStyle.SYNC) {
            for (ChangeListener changeListener : changeListeners) {
                try {
                    changeListener.changed(changeEvent);
                } catch (Exception e) {
                    e.printStackTrace();
                    Log.e(Log.TAG_SYNC, "Exception notifying replication listener: %s", e);
                }
            }
        } else {
            // ASYNC
            synchronized (workExecutor) {
                if (!workExecutor.isShutdown()) {
                    workExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                for (ChangeListener changeListener : changeListeners) {
                                    changeListener.changed(changeEvent);
                                }
                            } catch (Exception e) {
                                Log.e(Log.TAG_SYNC, "Exception notifying replication listener: %s", e);
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }
            }
        }
    }

    /**
     * Adds a change delegate that will be called whenever the Replication changes.
     */
    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener changeListener) {
        changeListeners.add(changeListener);
    }


    /**
     * Initialize the state machine which defines the overall behavior of the replication
     * object.
     */
    protected void initializeStateMachine() {

        stateMachine = new StateMachine<ReplicationState, ReplicationTrigger>(ReplicationState.INITIAL);

        // hierarchy
        stateMachine.configure(ReplicationState.IDLE).substateOf(ReplicationState.RUNNING);
        stateMachine.configure(ReplicationState.OFFLINE).substateOf(ReplicationState.RUNNING);

        // permitted transitions
        stateMachine.configure(ReplicationState.INITIAL).permit(
                ReplicationTrigger.START,
                ReplicationState.RUNNING
        );
        stateMachine.configure(ReplicationState.IDLE).permit(
                ReplicationTrigger.RESUME,
                ReplicationState.RUNNING
        );
        stateMachine.configure(ReplicationState.RUNNING).permit(
                ReplicationTrigger.WAITING_FOR_CHANGES,
                ReplicationState.IDLE
        );
        stateMachine.configure(ReplicationState.RUNNING).permit(
                ReplicationTrigger.STOP_IMMEDIATE,
                ReplicationState.STOPPED
        );
        stateMachine.configure(ReplicationState.RUNNING).permit(
                ReplicationTrigger.STOP_GRACEFUL,
                ReplicationState.STOPPING
        );
        stateMachine.configure(ReplicationState.RUNNING).permit(
                ReplicationTrigger.GO_OFFLINE,
                ReplicationState.OFFLINE
        );
        stateMachine.configure(ReplicationState.OFFLINE).permit(
                ReplicationTrigger.GO_ONLINE,
                ReplicationState.RUNNING
        );
        stateMachine.configure(ReplicationState.STOPPING).permit(
                ReplicationTrigger.STOP_IMMEDIATE,
                ReplicationState.STOPPED
        );

        // ignored transitions
        stateMachine.configure(ReplicationState.RUNNING).ignore(ReplicationTrigger.START);
        stateMachine.configure(ReplicationState.STOPPING).ignore(ReplicationTrigger.STOP_GRACEFUL);
        stateMachine.configure(ReplicationState.STOPPED).ignore(ReplicationTrigger.STOP_GRACEFUL);
        stateMachine.configure(ReplicationState.STOPPED).ignore(ReplicationTrigger.STOP_IMMEDIATE);
        stateMachine.configure(ReplicationState.STOPPING).ignore(ReplicationTrigger.WAITING_FOR_CHANGES);
        stateMachine.configure(ReplicationState.STOPPED).ignore(ReplicationTrigger.WAITING_FOR_CHANGES);
        stateMachine.configure(ReplicationState.OFFLINE).ignore(ReplicationTrigger.WAITING_FOR_CHANGES);
        stateMachine.configure(ReplicationState.INITIAL).ignore(ReplicationTrigger.GO_OFFLINE);
        stateMachine.configure(ReplicationState.STOPPING).ignore(ReplicationTrigger.GO_OFFLINE);
        stateMachine.configure(ReplicationState.STOPPED).ignore(ReplicationTrigger.GO_OFFLINE);
        stateMachine.configure(ReplicationState.OFFLINE).ignore(ReplicationTrigger.GO_OFFLINE);
        stateMachine.configure(ReplicationState.INITIAL).ignore(ReplicationTrigger.GO_ONLINE);
        stateMachine.configure(ReplicationState.RUNNING).ignore(ReplicationTrigger.GO_ONLINE);
        stateMachine.configure(ReplicationState.STOPPING).ignore(ReplicationTrigger.GO_ONLINE);
        stateMachine.configure(ReplicationState.STOPPED).ignore(ReplicationTrigger.GO_ONLINE);
        stateMachine.configure(ReplicationState.IDLE).ignore(ReplicationTrigger.GO_ONLINE);
        stateMachine.configure(ReplicationState.OFFLINE).ignore(ReplicationTrigger.RESUME);
        stateMachine.configure(ReplicationState.INITIAL).ignore(ReplicationTrigger.RESUME);
        stateMachine.configure(ReplicationState.RUNNING).ignore(ReplicationTrigger.RESUME);
        stateMachine.configure(ReplicationState.STOPPING).ignore(ReplicationTrigger.RESUME);
        stateMachine.configure(ReplicationState.STOPPED).ignore(ReplicationTrigger.RESUME);

        // actions
        stateMachine.configure(ReplicationState.RUNNING).onEntry(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                Log.v(Log.TAG_SYNC, "[onEntry()] " + transition.getSource() + " => " + transition.getDestination());
                ReplicationInternal.this.start();
                notifyChangeListenersStateTransition(transition);
            }
        });

        stateMachine.configure(ReplicationState.RUNNING).onExit(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                Log.v(Log.TAG_SYNC, "[onExit()] " + transition.getSource() + " => " + transition.getDestination());
            }
        });
        stateMachine.configure(ReplicationState.IDLE).onEntry(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                Log.v(Log.TAG_SYNC, "[onEntry()] " + transition.getSource() + " => " + transition.getDestination());
                retryReplicationIfError();
                if (transition.getSource() == transition.getDestination()) {
                    // ignore IDLE to IDLE
                    return;
                }
                notifyChangeListenersStateTransition(transition);

                // #352
                // iOS version: stop replicator immediately when call setError() with permanent error.
                // But, for Core Java, some of codes wait IDLE state. So this is reason to wait till
                // state becomes IDLE.
                if (Utils.isPermanentError(error) && isContinuous()) {
                    Log.d(Log.TAG_SYNC, "IDLE: triggerStop() " + error.toString());
                    triggerStop();
                }
            }
        });
        stateMachine.configure(ReplicationState.IDLE).onExit(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                Log.v(Log.TAG_SYNC, "[onExit()] " + transition.getSource() + " => " + transition.getDestination());
                if (transition.getSource() == transition.getDestination()) {
                    // ignore IDLE to IDLE
                    return;
                }
                notifyChangeListenersStateTransition(transition);
            }
        });
        stateMachine.configure(ReplicationState.OFFLINE).onEntry(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                Log.v(Log.TAG_SYNC, "[onEntry()] " + transition.getSource() + " => " + transition.getDestination());
                goOffline();
                notifyChangeListenersStateTransition(transition);
            }
        });
        stateMachine.configure(ReplicationState.OFFLINE).onExit(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                Log.v(Log.TAG_SYNC, "[onExit()] " + transition.getSource() + " => " + transition.getDestination());
                goOnline();
                notifyChangeListenersStateTransition(transition);
            }
        });
        stateMachine.configure(ReplicationState.STOPPING).onEntry(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                Log.v(Log.TAG_SYNC, "[onEntry()] " + transition.getSource() + " => " + transition.getDestination());

                // NOTE: Based on StateMachine configuration, this should not happen.
                //       However, from Unit Test result, this could be happen.
                //       We should revisit StateMachine configuration and also its Thread-safe-ability
                if (transition.getSource() == transition.getDestination()) {
                    // ignore STOPPING to STOPPING
                    return;
                }

                ReplicationInternal.this.stopGraceful();
                notifyChangeListenersStateTransition(transition);
            }
        });

        stateMachine.configure(ReplicationState.STOPPED).onEntry(new Action1<Transition<ReplicationState, ReplicationTrigger>>() {
            @Override
            public void doIt(Transition<ReplicationState, ReplicationTrigger> transition) {
                Log.v(Log.TAG_SYNC, "[onEntry()] " + transition.getSource() + " => " + transition.getDestination());
                saveLastSequence(); // move from databaseClosing() method as databaseClosing() is not called if Rem
                ReplicationInternal.this.clearDbRef();

                // close any active resources associated with this replicator
                close();

                // NOTE: Based on StateMachine configuration, this should not happen.
                //       However, from Unit Test result, this could be happen.
                //       We should revisit StateMachine configuration and also its Thread-safe-ability
                if (transition.getSource() == transition.getDestination()) {
                    // ignore STOPPED to STOPPED
                    return;
                }

                notifyChangeListenersStateTransition(transition);
            }
        });

    }

    private void logTransition(Transition<ReplicationState, ReplicationTrigger> transition) {
        Log.d(Log.TAG_SYNC, "State transition: %s -> %s (via %s).  this: %s", transition.getSource(), transition.getDestination(), transition.getTrigger(), this);
    }

    private void notifyChangeListenersStateTransition(Transition<ReplicationState, ReplicationTrigger> transition) {
        logTransition(transition);
        Replication.ChangeEvent changeEvent = new Replication.ChangeEvent(this);
        ReplicationStateTransition replicationStateTransition = new ReplicationStateTransition(transition);
        changeEvent.setTransition(replicationStateTransition);
        notifyChangeListeners(changeEvent);
    }

    /**
     * A delegate that can be used to listen for Replication changes.
     */
    @InterfaceAudience.Public
    public static interface ChangeListener {
        public void changed(Replication.ChangeEvent event);
    }

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    @InterfaceAudience.Private
    protected boolean serverIsSyncGatewayVersion(String minVersion) {
        String prefix = "Couchbase Sync Gateway/";
        if (serverType == null) {
            return false;
        } else {
            if (serverType.startsWith(prefix)) {
                String versionString = serverType.substring(prefix.length());
                return versionString.compareTo(minVersion) >= 0;
            }
        }
        return false;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void addToInbox(RevisionInternal rev) {
        Log.v(Log.TAG_SYNC, "%s: addToInbox() called, rev: %s.  Thread: %s", this, rev, Thread.currentThread());
        batcher.queueObject(rev);
    }

    /**
     * Called after a continuous replication has gone idle, but it failed to transfer some revisions
     * and so wants to try again in a minute. Can be overridden by subclasses.
     * <p/>
     * in CBL_Replicator.m
     * - (void) retry
     */
    protected void retry() {
        Log.v(Log.TAG_SYNC, "[retry()]");
        retryCount++;
        error = null;
        checkSession();
    }

    /**
     * in CBL_Replicator.m
     * - (void) retryIfReady
     */
    protected void retryIfReady() {
        Log.v(Log.TAG_SYNC, "[retryIfReady()] stateMachine => " + stateMachine.getState().toString());
        // check if state is still IDLE (ONLINE), then retry now.
        if (stateMachine.getState().equals(ReplicationState.IDLE)) {
            Log.v(Log.TAG_SYNC, "%s RETRYING, to transfer missed revisions...", this);
            cancelRetryFuture();
            retry();
        }
    }

    /**
     * helper function to schedule retry future. no in iOS code.
     */
    private void scheduleRetryFuture() {
        long delay = RETRY_DELAY_SECONDS * (long) Math.pow((double) 2, (double) Math.min(retryCount, MAX_RETRIES));
        Log.v(Log.TAG_SYNC, "%s: Failed to xfer; will retry in %d sec",
                this, delay);
        this.retryFuture = workExecutor.schedule(new Runnable() {
            public void run() {
                retryIfReady();
            }
        }, delay, TimeUnit.SECONDS);
    }

    /**
     * helper function to cancel retry future. not in iOS code.
     */
    private void cancelRetryFuture() {
        if (retryFuture != null && !retryFuture.isDone()) {
            retryFuture.cancel(true);
        }
        retryFuture = null;
    }

    /**
     * Retry replication if previous attempt ends with error
     */
    protected void retryReplicationIfError() {
        // Make sure if state is IDLE, this method should be called when state becomes IDLE
        if (!stateMachine.getState().equals(ReplicationState.IDLE)) {
            return;
        }

        // IDLE_OK
        if (error == null) {
            retryCount = 0;
        }
        // IDLE_ERROR
        else {
            // not retry infinite times
            if (retryCount < MAX_RETRIES) {
                // mode should be continuous
                if (isContinuous()) {
                    // 12/16/2014 - only retry if error is transient error 50x http error
                    // It may need to retry for any kind of errors
                    if (Utils.isTransientError(error)) {

                        cancelRetryFuture();
                        scheduleRetryFuture();
                    }
                }
            }
        }
    }

    @InterfaceAudience.Private
    protected void setServerType(String serverType) {
        this.serverType = serverType;
    }

    public Replication.Lifecycle getLifecycle() {
        return lifecycle;
    }

    public void setLifecycle(Replication.Lifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    private static int SAVE_LAST_SEQUENCE_DELAY = 5; // 5 sec;

    /**
     * in CBL_Replicator.m
     * - (void) setLastSequence:(NSString*)lastSequence;
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public void setLastSequence(String lastSequenceIn) {
        if (lastSequenceIn != null && !lastSequenceIn.equals(lastSequence)) {
            Log.v(Log.TAG_SYNC, "%s: Setting lastSequence to %s from(%s)", this, lastSequenceIn, lastSequence);
            lastSequence = lastSequenceIn;
            if (!lastSequenceChanged) {
                lastSequenceChanged = true;
                workExecutor.schedule(new Runnable() {
                    public void run() {
                        saveLastSequence();
                    }
                }, SAVE_LAST_SEQUENCE_DELAY, TimeUnit.SECONDS);
            }
        }
    }

    protected RevisionInternal transformRevision(RevisionInternal rev) {
        if (revisionBodyTransformationBlock != null) {
            try {
                final int generation = rev.getGeneration();
                RevisionInternal xformed = revisionBodyTransformationBlock.invoke(rev);
                if (xformed == null)
                    return null;
                if (xformed != rev) {
                    assert (xformed.getDocID().equals(rev.getDocID()));
                    assert (xformed.getRevID().equals(rev.getRevID()));
                    assert (xformed.getProperties().get("_revisions").equals(rev.getProperties().get("_revisions")));
                    if (xformed.getProperties().get("_attachments") != null) {
                        // Insert 'revpos' properties into any attachments added by the callback:
                        RevisionInternal mx = new RevisionInternal(xformed.getProperties());
                        xformed = mx;
                        mx.mutateAttachments(new CollectionUtils.Functor<Map<String, Object>, Map<String, Object>>() {
                            public Map<String, Object> invoke(Map<String, Object> info) {
                                if (info.get("revpos") != null) {
                                    return info;
                                }
                                if (info.get("data") == null) {
                                    throw new IllegalStateException("Transformer added attachment without adding data");
                                }
                                Map<String, Object> nuInfo = new HashMap<String, Object>(info);
                                nuInfo.put("revpos", generation);
                                return nuInfo;
                            }
                        });
                    }
                    rev = xformed;
                }
            } catch (Exception e) {
                Log.w(Log.TAG_SYNC, "%s: Exception transforming a revision of doc '%s", e, this, rev.getDocID());
            }
        }
        return rev;
    }

    @InterfaceAudience.Private
    protected static Status statusFromBulkDocsResponseItem(Map<String, Object> item) {

        try {
            if (!item.containsKey("error")) {
                return new Status(Status.OK);
            }
            String errorStr = (String) item.get("error");
            if (errorStr == null || errorStr.isEmpty()) {
                return new Status(Status.OK);
            }

            // 'status' property is nonstandard; Couchbase Lite returns it, others don't.
            Object objStatus = item.get("status");
            if (objStatus instanceof Integer) {
                int status = ((Integer) objStatus).intValue();
                if (status >= 400) {
                    return new Status(status);
                }
            }
            // If no 'status' present, interpret magic hardcoded CouchDB error strings:
            if (errorStr.equalsIgnoreCase("unauthorized")) {
                return new Status(Status.UNAUTHORIZED);
            } else if (errorStr.equalsIgnoreCase("forbidden")) {
                return new Status(Status.FORBIDDEN);
            } else if (errorStr.equalsIgnoreCase("conflict")) {
                return new Status(Status.CONFLICT);
            } else if (errorStr.equalsIgnoreCase("missing")) {
                return new Status(Status.NOT_FOUND);
            } else if (errorStr.equalsIgnoreCase("not_found")) {
                return new Status(Status.NOT_FOUND);
            } else {
                return new Status(Status.UPSTREAM_ERROR);
            }

        } catch (Exception e) {
            Log.e(Database.TAG, "Exception getting status from " + item, e);
        }
        return new Status(Status.OK);


    }

    private void clearDbRef() {

        // TODO: there was some logic here that was NOT saving the checkpoint to
        // TODO: the DB if: (savingCheckpoint && lastSequence != null && db != null)

        try {
            Log.v(Log.TAG_SYNC, "%s: clearDbRef() called", this);

            if (!db.isOpen()) {
                Log.w(Log.TAG_SYNC, "Not attempting to setLastSequence, db is closed");
            } else {
                db.setLastSequence(lastSequence, remoteCheckpointDocID());
            }
            Log.v(Log.TAG_SYNC, "%s: clearDbRef() setting db to null", this);
            db = null;

        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, "Exception in clearDbRef(): %s", e);
        }
    }

    /**
     * For java docs, see Replication.setCookie()
     */
    public void setCookie(String name, String value, String path, long maxAge, boolean secure, boolean httpOnly) {
        Date now = new Date();
        Date expirationDate = new Date(now.getTime() + maxAge);
        setCookie(name, value, path, expirationDate, secure, httpOnly);
    }


    /**
     * For java docs, see Replication.setCookie()
     */
    public void setCookie(String name, String value, String path, Date expirationDate, boolean secure, boolean httpOnly) {
        if (remote == null) {
            throw new IllegalStateException("Cannot setCookie since remote == null");
        }
        BasicClientCookie2 cookie = new BasicClientCookie2(name, value);
        cookie.setDomain(remote.getHost());
        if (path != null && path.length() > 0) {
            cookie.setPath(path);
        } else {
            cookie.setPath(remote.getPath());
        }

        cookie.setExpiryDate(expirationDate);
        cookie.setSecure(secure);
        List<Cookie> cookies = Arrays.asList((Cookie) cookie);
        this.clientFactory.addCookies(cookies);

    }

    /**
     * For java docs, see Replication.deleteCookie()
     */
    public void deleteCookie(String name) {
        this.clientFactory.deleteCookie(name);
    }

    protected HttpClientFactory getClientFactory() {
        return clientFactory;
    }

    /**
     * For javadocs, see Replication object
     */
    public List<String> getChannels() {
        if (filterParams == null || filterParams.isEmpty()) {
            return new ArrayList<String>();
        }
        String params = (String) filterParams.get(CHANNELS_QUERY_PARAM);
        if (!isPull() || getFilter() == null || !getFilter().equals(BY_CHANNEL_FILTER_NAME) || params == null || params.isEmpty()) {
            return new ArrayList<String>();
        }
        String[] paramsArray = params.split(",");
        return new ArrayList<String>(Arrays.asList(paramsArray));
    }

    /**
     * For javadocs, see Replication object
     */
    public void setChannels(List<String> channels) {
        if (channels != null && !channels.isEmpty()) {
            if (!isPull()) {
                Log.w(Log.TAG_SYNC, "filterChannels can only be set in pull replications");
                return;
            }
            setFilter(BY_CHANNEL_FILTER_NAME);
            Map<String, Object> filterParams = new HashMap<String, Object>();
            filterParams.put(CHANNELS_QUERY_PARAM, TextUtils.join(",", channels));
            setFilterParams(filterParams);
        } else if (getFilter().equals(BY_CHANNEL_FILTER_NAME)) {
            setFilter(null);
            setFilterParams(null);
        }
    }

    public String getSessionID() {
        return sessionID;
    }

    public abstract void waitForPendingFutures();

    //@Override
    //public void changed(EventType type, Object o, BlockingQueue queue) {
    // should be overide
    //}
    @Override
    public void changed(EventType type, Object o, BlockingQueue queue) {
        // Log.d(Log.TAG_SYNC, "[changed()] " + type + " size="+queue.size());

        if (type == EventType.PUT || type == EventType.ADD) {
            // in case of one shot, not necessary to switch state and call waitForPendingFutures.
            if (isContinuous()) {
                if (!queue.isEmpty()) {
                    // trigger to RUNNING if state is IDLE
                    fireTrigger(ReplicationTrigger.RESUME);

                    // run waitForPendingFutures.
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            waitForPendingFutures();
                        }
                    }).start();
                }
            }
        }
    }

    /**
     * Encodes the given document id for use in an URI.
     * <p/>
     * Avoids encoding the slash in _design documents since it may cause a 301 redirect.
     */
    protected String encodeDocumentId(String docId) {
        if (docId.startsWith("_design/")) {
            // http://docs.couchdb.org/en/1.6.1/http-api.html#cap-/{db}/_design/{ddoc}
            String designDocId = docId.substring("_design/".length());
            return "_design/".concat(URIUtils.encode(designDocId));
        } else {
            return URIUtils.encode(docId);
        }
    }
}
