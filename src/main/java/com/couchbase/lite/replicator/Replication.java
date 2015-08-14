package com.couchbase.lite.replicator;

import com.couchbase.lite.AsyncTask;
import com.couchbase.lite.Database;
import com.couchbase.lite.Document;
import com.couchbase.lite.Manager;
import com.couchbase.lite.NetworkReachabilityListener;
import com.couchbase.lite.ReplicationFilter;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.support.CouchbaseLiteHttpClientFactory;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.PersistentCookieStore;
import com.couchbase.lite.util.Log;

import java.net.URL;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * The external facade for the Replication API
 */
public class Replication implements ReplicationInternal.ChangeListener, NetworkReachabilityListener {

    /**
     * Enum to specify which direction this replication is going (eg, push vs pull)
     *
     * @exclude
     */
    public enum Direction { PULL, PUSH }

    /**
     * Enum to specify whether this replication is oneshot or continuous.
     *
     * @exclude
     */
    public enum Lifecycle { ONESHOT, CONTINUOUS }

    /**
     * @exclude
     */
    public static final String REPLICATOR_DATABASE_NAME = "_replicator";

    public static final long DEFAULT_MAX_TIMEOUT_FOR_SHUTDOWN = 60; // 60 sec

    /**
     * Options for what metadata to include in document bodies
     */
    public enum ReplicationStatus {
        /** The replication is finished or hit a fatal error. */
        REPLICATION_STOPPED,
        /** The remote host is currently unreachable. */
        REPLICATION_OFFLINE,
        /** Continuous replication is caught up and waiting for more changes.*/
        REPLICATION_IDLE,
        /** The replication is actively transferring data. */
        REPLICATION_ACTIVE
    }

    protected Database db;
    protected URL remote;
    protected HttpClientFactory clientFactory;
    protected ScheduledExecutorService workExecutor;
    protected ReplicationInternal replicationInternal;
    protected Lifecycle lifecycle;
    protected List<ChangeListener> changeListeners;
    protected Throwable lastError;
    protected Direction direction;

    private Object lockPendingDocIDs = new Object();
    private Set<String> pendingDocIDs;

    public enum ReplicationField {
        FILTER_NAME,
        FILTER_PARAMS,
        DOC_IDS,
        REQUEST_HEADERS,
        AUTHENTICATOR,
        CREATE_TARGET
    }

    /**
     * Properties of the replicator that are saved across restarts
     */
    protected Map<ReplicationField, Object> properties;

    /**
     * Constructor
     * @exclude
     */
    @InterfaceAudience.Private
    public Replication(Database db, URL remote, Direction direction) {
        this(
                db,
                remote,
                direction,
                db.getManager().getDefaultHttpClientFactory(),
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "CBLReplicationWorker");
                    }
                })
        );
    }

    /**
     * Constructor
     * @exclude
     */
    @InterfaceAudience.Private
    public Replication(Database db, URL remote, Direction direction, HttpClientFactory clientFactory, ScheduledExecutorService workExecutor) {

        this.db = db;
        this.remote = remote;
        this.workExecutor = workExecutor;
        this.changeListeners = new CopyOnWriteArrayList<ChangeListener>();
        this.lifecycle = Lifecycle.ONESHOT;
        this.direction = direction;
        this.properties = new EnumMap<ReplicationField, Object>(ReplicationField.class);

        setClientFactory(clientFactory);

        initReplicationInternal();
    }

    private void initReplicationInternal() {
        switch (direction) {
            case PULL:
                replicationInternal = new PullerInternal(
                        this.db,
                        this.remote,
                        this.clientFactory,
                        this.workExecutor,
                        this.lifecycle,
                        this
                );
                break;
            case PUSH:
                replicationInternal = new PusherInternal(
                        this.db,
                        this.remote,
                        this.clientFactory,
                        this.workExecutor,
                        this.lifecycle,
                        this
                );
                break;
            default:
                throw new RuntimeException(String.format("Unknown direction: %s", direction));
        }

        addProperties(replicationInternal);

        replicationInternal.addChangeListener(this);
    }

    /**
     * Starts the replication, asynchronously.
     */
    @InterfaceAudience.Public
    public void start() {

        if (replicationInternal == null) {
            initReplicationInternal();
        } else {
            if (replicationInternal.stateMachine.isInState(ReplicationState.INITIAL)) {
                // great, it's ready to be started, nothing to do
            } else if (replicationInternal.stateMachine.isInState(ReplicationState.STOPPED)) {
                // if there was a previous internal replication and it's in the STOPPED state, then
                // start a fresh internal replication
                initReplicationInternal();
            } else {
                String mesg = String.format("replicationInternal in unexpected state: %s, ignoring start()", replicationInternal.stateMachine.getState());
                Log.w(Log.TAG_SYNC, mesg);
            }
        }

        // forget cached IDs (Should be executed in workExecutor)
        if (pendingDocIDs != null) {
            db.runAsync(new AsyncTask() {
                @Override
                public void run(Database database) {
                    synchronized (lockPendingDocIDs) {
                        pendingDocIDs = null;
                    }
                }
            });
        }


        replicationInternal.triggerStart();
    }

    /**
     * Restarts the replication.  This blocks until the replication successfully stops.
     *
     * Alternatively, you can stop() the replication and create a brand new one and start() it.
     */
    @InterfaceAudience.Public
    public void restart() {

        // stop replicator if necessary
        if(this.isRunning()) {
            final CountDownLatch stopped = new CountDownLatch(1);
            ChangeListener listener = new ChangeListener() {
                @Override
                public void changed(ChangeEvent event) {
                    if (event.getTransition() != null && event.getTransition().getDestination() == ReplicationState.STOPPED) {
                        stopped.countDown();
                    }
                }
            };
            addChangeListener(listener);

            // tries to stop replicator
            stop();

            try {
                // If need to wait more than 60 sec to stop, throws Exception
                boolean ret = stopped.await(60, TimeUnit.SECONDS);
                if(ret == false){
                    throw new RuntimeException("Replicator is unable to stop.");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finally {
                removeChangeListener(listener);
            }
        }

        // start replicator
        start();
    }

    /**
     * Tell the replication to go offline, asynchronously.
     */
    @InterfaceAudience.Public
    public void goOffline() {
        replicationInternal.triggerGoOffline();
    }

    /**
     * Tell the replication to go online, asynchronously.
     */
    @InterfaceAudience.Public
    public void goOnline() {
        replicationInternal.triggerGoOnline();
    }

    /**
     * True while the replication is running, False if it's stopped.
     * Note that a continuous replication never actually stops; it only goes idle waiting for new
     * data to appear.
     */
    @InterfaceAudience.Public
    public boolean isRunning() {
        if (replicationInternal == null) {
            return false;
        }
        return replicationInternal.stateMachine.isInState(ReplicationState.RUNNING) ||
                replicationInternal.stateMachine.isInState(ReplicationState.IDLE) ||
                replicationInternal.stateMachine.isInState(ReplicationState.OFFLINE);
    }

    /**
     * Stops the replication, asynchronously.
     */
    @InterfaceAudience.Public
    public void stop() {
        if (replicationInternal != null) {
            replicationInternal.triggerStop();
        }
    }

    /**
     * Is this replication continous?
     */
    @InterfaceAudience.Public
    public boolean isContinuous() {
        return lifecycle == Lifecycle.CONTINUOUS;
    }

    /**
     * Set whether this replication is continous
     */
    @InterfaceAudience.Public
    public void setContinuous(boolean isContinous) {
        if (isContinous) {
            this.lifecycle = Lifecycle.CONTINUOUS;
            replicationInternal.setLifecycle(Lifecycle.CONTINUOUS);
        } else {
            this.lifecycle = Lifecycle.ONESHOT;
            replicationInternal.setLifecycle(Lifecycle.ONESHOT);
        }
    }

    /**
     * Set the Authenticator used for authenticating with the Sync Gateway
     */
    @InterfaceAudience.Public
    public void setAuthenticator(Authenticator authenticator) {
        properties.put(ReplicationField.AUTHENTICATOR, authenticator);
        replicationInternal.setAuthenticator(authenticator);
    }

    /**
     * Get the Authenticator used for authenticating with the Sync Gateway
     */
    @InterfaceAudience.Public
    public Authenticator getAuthenticator() {
        return replicationInternal.getAuthenticator();
    }

    /**
     * Should the target database be created if it doesn't already exist? (Defaults to NO).
     */
    @InterfaceAudience.Public
    public boolean shouldCreateTarget() {
        return replicationInternal.shouldCreateTarget();
    }

    /**
     * Set whether the target database be created if it doesn't already exist?
     */
    @InterfaceAudience.Public
    public void setCreateTarget(boolean createTarget) {
        properties.put(ReplicationField.CREATE_TARGET, createTarget);
        replicationInternal.setCreateTarget(createTarget);
    };

    /**
     * Adds a change delegate that will be called whenever the Replication changes.
     */
    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener changeListener) {
        changeListeners.add(changeListener);
    }

    /**
     * Removes the specified delegate as a listener for the Replication change event.
     */
    @InterfaceAudience.Public
    public void removeChangeListener(ChangeListener changeListener) {
        changeListeners.remove(changeListener);
    }

    /**
     * The replication's current state, one of {stopped, offline, idle, active}.
     */
    @InterfaceAudience.Public
    public ReplicationStatus getStatus() {
        if (replicationInternal == null) {
            return ReplicationStatus.REPLICATION_STOPPED;
        } else if (replicationInternal.stateMachine.isInState(ReplicationState.OFFLINE)) {
            return ReplicationStatus.REPLICATION_OFFLINE;
        } else if (replicationInternal.stateMachine.isInState(ReplicationState.IDLE)) {
            return ReplicationStatus.REPLICATION_IDLE;
        } else if (replicationInternal.stateMachine.isInState(ReplicationState.STOPPED)) {
            return ReplicationStatus.REPLICATION_STOPPED;
        } else {
            return ReplicationStatus.REPLICATION_ACTIVE;
        }
    }

    /**
     * This is called back for changes from the ReplicationInternal.
     * Simply propagate the events back to all listeners.
     */
    @Override
    public void changed(ChangeEvent event) {

        // forget cached IDs (Should be executed in workExecutor)
        if (pendingDocIDs != null) {
            db.runAsync(new AsyncTask() {
                @Override
                public void run(Database database) {
                    synchronized (lockPendingDocIDs) {
                        pendingDocIDs = null;
                    }
                }
            });
        }

        for (ChangeListener changeListener : changeListeners) {
            try {
                changeListener.changed(event);
            } catch (Exception e) {
                Log.e(Log.TAG_SYNC, "Exception calling changeListener.changed", e);
            }
        }
    }

    /**
     * The error status of the replication, or null if there have not been any errors since
     * it started.
     */
    @InterfaceAudience.Public
    public Throwable getLastError() {
        return lastError;
    }

    /**
     * The number of completed changes processed, if the task is active, else 0 (observable).
     */
    @InterfaceAudience.Public
    public int getCompletedChangesCount() {
        return replicationInternal.getCompletedChangesCount().get();
    }

    /**
     * The total number of changes to be processed, if the task is active, else 0 (observable).
     */
    @InterfaceAudience.Public
    public int getChangesCount() {
        return replicationInternal.getChangesCount().get();
    }

    /**
     * Update the lastError
     */
    protected void setLastError(Throwable lastError) {
        this.lastError = lastError;
    }

    /**
     * Following two methods for temporary methods instead of CBL_ReplicatorSettings implementation.
     */
    protected String remoteCheckpointDocID() {
        return replicationInternal.remoteCheckpointDocID();
    }

    public Set<String> getPendingDocumentIDs() {
        synchronized (lockPendingDocIDs) {
            if (isPull() || (isRunning() && pendingDocIDs != null))
                return pendingDocIDs;

            final CountDownLatch latch = new CountDownLatch(1);
            this.workExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        String filterName = (String) properties.get("filter");
                        Map<String, Object> filterParams = (Map<String, Object>) properties.get("query_params");
                        ReplicationFilter filter = null;
                        if (filterName != null) {
                            filter = db.getFilter(filterName);
                        }

                        String lastSequence = replicationInternal.lastSequence;
                        if (lastSequence == null)
                            lastSequence = db.lastSequenceWithCheckpointId(remoteCheckpointDocID());
                        RevisionList revs = db.unpushedRevisionsSince(lastSequence, filter, filterParams);
                        if (revs != null) {
                            pendingDocIDs = new HashSet<String>();
                            pendingDocIDs.addAll(revs.getAllDocIds());
                        } else
                            pendingDocIDs = null;
                    } finally {
                        latch.countDown();
                    }
                }
            });

            try {
                latch.await();
            } catch (InterruptedException e) {
                Log.e(Log.TAG_SYNC, "InterruptedException", e);
            }

            return pendingDocIDs;
        }
    }

    public boolean isDocumentPending(Document doc){
        if(doc == null) return false;

        // getPendingDocumentIDs() is not simple getter. so not cheap.
        Set<String> ids = getPendingDocumentIDs();
        if(ids == null) return false;

        return ids.contains(doc.getId());
    }

    /**
     * A delegate that can be used to listen for Replication changes.
     */
    @InterfaceAudience.Public
    public interface ChangeListener {
        void changed(ChangeEvent event);
    }

    /**
     * The type of event raised by a Replication when any of the following
     * properties change: mode, running, error, completed, total.
     */
    @InterfaceAudience.Public
    public static class ChangeEvent {

        private Replication source;
        private ReplicationStateTransition transition;
        private int changeCount;
        private int completedChangeCount;
        private Throwable error;

        protected ChangeEvent(ReplicationInternal replInternal) {
            this.source = replInternal.parentReplication;
            this.changeCount = replInternal.getChangesCount().get();
            this.completedChangeCount =replInternal.getCompletedChangesCount().get();
        }

        /**
         * Get the owner Replication object that generated this ChangeEvent.
         */
        public Replication getSource() {
            return source;
        }

        /**
         * Get the ReplicationStateTransition associated with this ChangeEvent, or nil
         * if it was not associated with a state transition.
         *
         * This is not in our official public API, and is subject to change/removal.
         */
        public ReplicationStateTransition getTransition() {
            return transition;
        }

        protected void setTransition(ReplicationStateTransition transition) {
            this.transition = transition;
        }

        /**
         * The total number of changes to be processed, if the task is active, else 0.
         *
         * This is not in our official public API, and is subject to change/removal.
         */
        public int getChangeCount() {
            return changeCount;
        }

        /**
         * The number of completed changes processed, if the task is active, else 0.
         *
         * This is not in our official public API, and is subject to change/removal.
         */
        public int getCompletedChangeCount() {
            return completedChangeCount;
        }

        /**
         * Get the error that triggered this callback, if any.  There also might
         * be a non-null error saved by the replicator from something that previously
         * happened, which you can get by calling getSource().getLastError().
         */
        public Throwable getError() {
            return error;
        }

        protected void setError(Throwable error) {
            this.error = error;
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(getSource().direction);
            sb.append(" replication event. Source: ");
            sb.append(getSource());
            if (getTransition() != null) {
                sb.append(" Transition: ");
                sb.append(getTransition().getSource());
                sb.append(" -> ");
                sb.append(getTransition().getDestination());
            }
            sb.append(" Total changes: ");
            sb.append(getChangeCount());
            sb.append(" Completed changes: ");
            sb.append(getCompletedChangeCount());
            return sb.toString();
        }
    }

    /**
     * Set the HTTP client factory if one was passed in, or use the default
     * set in the manager if available.
     * @param clientFactory
     */
    @InterfaceAudience.Private
    protected void setClientFactory(HttpClientFactory clientFactory) {
        Manager manager = null;
        if (this.db != null) {
            manager = this.db.getManager();
        }
        HttpClientFactory managerClientFactory = null;
        if (manager != null) {
            managerClientFactory = manager.getDefaultHttpClientFactory();
        }
        if (clientFactory != null) {
            this.clientFactory = clientFactory;
        } else {
            if (managerClientFactory != null) {
                this.clientFactory = managerClientFactory;
            } else {
                PersistentCookieStore cookieStore = db.getPersistentCookieStore();
                this.clientFactory = new CouchbaseLiteHttpClientFactory(cookieStore);
            }
        }
    }

    @InterfaceAudience.Private
    protected boolean serverIsSyncGatewayVersion(String minVersion) {
        return replicationInternal.serverIsSyncGatewayVersion(minVersion);
    }

    @InterfaceAudience.Private
    protected void setServerType(String serverType) {
        replicationInternal.setServerType(serverType);
    }

    /**
     * Set the filter to be used by this replication
     */
    @InterfaceAudience.Public
    public void setFilter(String filterName) {
        properties.put(ReplicationField.FILTER_NAME, filterName);
        replicationInternal.setFilter(filterName);
    }

    /**
     * Sets the documents to specify as part of the replication.
     */
    @InterfaceAudience.Public
    public void setDocIds(List<String> docIds) {
        properties.put(ReplicationField.DOC_IDS, docIds);
        replicationInternal.setDocIds(docIds);
    }

    /**
     * Gets the documents to specify as part of the replication.
     */
    public List<String> getDocIds() {
        return replicationInternal.getDocIds();
    }

    /**
     * Set parameters to pass to the filter function.
     */
    public void setFilterParams(Map<String, Object> filterParams) {
        properties.put(ReplicationField.FILTER_PARAMS, filterParams);
        replicationInternal.setFilterParams(filterParams);
    }

    /**
     * Sets an HTTP cookie for the Replication.
     *
     * @param name The name of the cookie.
     * @param value The value of the cookie.
     * @param path The path attribute of the cookie.  If null or empty, will use remote.getPath()
     * @param maxAge The maxAge, in milliseconds, that this cookie should be valid for.
     * @param secure Whether the cookie should only be sent using a secure protocol (e.g. HTTPS).
     * @param httpOnly (ignored) Whether the cookie should only be used when transmitting HTTP, or HTTPS, requests thus restricting access from other, non-HTTP APIs.
     */
    @InterfaceAudience.Public
    public void setCookie(String name, String value, String path, long maxAge, boolean secure, boolean httpOnly) {
        replicationInternal.setCookie(name, value, path, maxAge, secure, httpOnly);
    }

    /**
     * Sets an HTTP cookie for the Replication.
     *
     * @param name The name of the cookie.
     * @param value The value of the cookie.
     * @param path The path attribute of the cookie.  If null or empty, will use remote.getPath()
     * @param expirationDate The expiration date of the cookie.
     * @param secure Whether the cookie should only be sent using a secure protocol (e.g. HTTPS).
     * @param httpOnly (ignored) Whether the cookie should only be used when transmitting HTTP, or HTTPS, requests thus restricting access from other, non-HTTP APIs.
     */
    @InterfaceAudience.Public
    public void setCookie(String name, String value, String path, Date expirationDate, boolean secure, boolean httpOnly) {
        replicationInternal.setCookie(name, value, path, expirationDate, secure, httpOnly);
    }

    /**
     * Deletes an HTTP cookie for the Replication.
     *
     * @param name The name of the cookie.
     */
    @InterfaceAudience.Public
    public void deleteCookie(String name) {
        replicationInternal.deleteCookie(name);
    }

    @InterfaceAudience.Private
    protected HttpClientFactory getClientFactory() {
        return replicationInternal.getClientFactory();
    }

    @InterfaceAudience.Private
    protected String buildRelativeURLString(String relativePath) {
        return replicationInternal.buildRelativeURLString(relativePath);
    }

    /**
     * List of Sync Gateway channel names to filter by; a nil value means no filtering, i.e. all
     * available channels will be synced.  Only valid for pull replications whose source database
     * is on a Couchbase Sync Gateway server.  (This is a convenience that just reads or
     * changes the values of .filter and .query_params.)
     */
    @InterfaceAudience.Public
    public List<String> getChannels() {
        return replicationInternal.getChannels();
    }

    /**
     * Set the list of Sync Gateway channel names
     */
    @InterfaceAudience.Public
    public void setChannels(List<String> channels) {
        replicationInternal.setChannels(channels);
    }

    /**
     * Name of an optional filter function to run on the source server. Only documents for
     * which the function returns true are replicated.
     *
     * For a pull replication, the name looks like "designdocname/filtername".
     * For a push replication, use the name under which you registered the filter with the Database.
     */
    @InterfaceAudience.Public
    public String getFilter() {
        return replicationInternal.getFilter();
    }

    /**
     * Parameters to pass to the filter function.  Should map strings to strings.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getFilterParams() {
        return replicationInternal.getFilterParams();
    }

    /**
     * Set Extra HTTP headers to be sent in all requests to the remote server.
     */
    @InterfaceAudience.Public
    public void setHeaders(Map<String, Object> requestHeadersParam) {
        properties.put(ReplicationField.REQUEST_HEADERS, requestHeadersParam);
        replicationInternal.setHeaders(requestHeadersParam);
    }

    /**
     * Extra HTTP headers to send in all requests to the remote server.
     * Should map strings (header names) to strings.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getHeaders() {
        return replicationInternal.getHeaders();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void databaseClosing() {
        replicationInternal.databaseClosing();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getSessionID() {
        return replicationInternal.getSessionID();
    }

    /**
     * Get the local database which is the source or target of this replication
     */
    @InterfaceAudience.Public
    public Database getLocalDatabase() {
        return db;
    }

    /**
     * Get the remote URL which is the source or target of this replication
     */
    @InterfaceAudience.Public
    public URL getRemoteUrl() {
        return remote;
    }

    /**
     * Is this a pull replication?  (Eg, it pulls data from Sync Gateway -> Device running CBL?)
     */
    @InterfaceAudience.Public
    public boolean isPull() {
        return replicationInternal.isPull();
    }

    @Override
    @InterfaceAudience.Private
    public void networkReachable() {
        goOnline();
    }

    @Override
    @InterfaceAudience.Private
    public void networkUnreachable() {
        goOffline();
    }

    /**
     * Add any properties associated with this Replication to the given
     * ReplicationInternal object -- currently used to preserve properties
     * of a Replication across restarts of the ReplicationInternal object.
     *
     * @param replicationInternal
     */
    private void addProperties(ReplicationInternal replicationInternal) {

        for (ReplicationField key : properties.keySet()) {

            Object value = properties.get(key);

            switch (key) {
                case FILTER_NAME:
                    replicationInternal.setFilter((String)value);
                    break;
                case FILTER_PARAMS:
                    replicationInternal.setFilterParams((Map)value);
                    break;
                case DOC_IDS:
                    replicationInternal.setDocIds((List)value);
                    break;
                case AUTHENTICATOR:
                    replicationInternal.setAuthenticator((Authenticator)value);
                    break;
                case CREATE_TARGET:
                    replicationInternal.setCreateTarget((Boolean)value);
                    break;
                case REQUEST_HEADERS:
                    replicationInternal.setHeaders((Map)value);
                    break;
            }
        }
    }
}
