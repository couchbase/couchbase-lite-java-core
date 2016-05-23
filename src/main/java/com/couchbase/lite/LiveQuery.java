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
package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Query subclass that automatically refreshes the result rows every time the database changes.
 * All you need to do is use add a listener to observe changes.
 */
public final class LiveQuery extends Query implements Database.ChangeListener {

    private boolean observing;
    private QueryEnumerator rows;
    private List<ChangeListener> observers = new ArrayList<ChangeListener>();
    private Throwable lastError;
    private final AtomicBoolean runningState; // true == running, false == stopped

    /**
     * If a query is running and the user calls stop() on this query, the future
     * will be used in order to cancel the query in progress.
     */
    protected Future queryFuture;

    /**
     * If the update() method is called while a query is in progress, once it is
     * finished it will be scheduled to re-run update().  This tracks the future
     * related to that scheduled task.
     */
    protected Future rerunUpdateFuture;

    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ LiveQuery(Query query) {
        super(query.getDatabase(), query);
        runningState = new AtomicBoolean(false);
    }

    /**
     * Sends the query to the server and returns an enumerator over the result rows (Synchronous).
     * Note: In a LiveQuery you should consider adding a ChangeListener and calling start() instead.
     */
    @Override
    @InterfaceAudience.Public
    public QueryEnumerator run() throws CouchbaseLiteException {
        waitForRows();

        if (rows == null) {
            return null;
        } else {
            // Have to return a copy because the enumeration has to start at item #0 every time
            return new QueryEnumerator(rows);
        }
    }

    /**
     * Returns the last error, if any, that occured while executing the Query, otherwise null.
     */
    @InterfaceAudience.Public
    public Throwable getLastError() {
        return lastError;
    }

    /**
     * Starts observing database changes. The .rows property will now update automatically. (You
     * usually don't need to call this yourself, since calling getRows() will start it for you
     */
    @InterfaceAudience.Public
    public void start() {

        if (runningState.get() == true) {
            Log.v(Log.TAG_QUERY, "%s: start() called, but runningState is already true.  Ignoring.", this);
            return;
        } else {
            Log.d(Log.TAG_QUERY, "%s: start() called", this);
            runningState.set(true);
        }

        if (!observing) {
            observing = true;
            getDatabase().addChangeListener(this);
            Log.v(Log.TAG_QUERY, "%s: start() is calling update()", this);
            update();
        }
    }

    /**
     * Stops observing database changes. Calling start() or rows() will restart it.
     */
    @InterfaceAudience.Public
    public void stop() {

        if (runningState.get() == false) {
            Log.d(Log.TAG_QUERY, "%s: stop() called, but runningState is already false.  Ignoring.", this);
            return;
        } else {
            Log.d(Log.TAG_QUERY, "%s: stop() called", this);
            runningState.set(false);
        }

        if (observing) {
            observing = false;
            getDatabase().removeChangeListener(this);
        }

        // slight diversion from iOS version -- cancel the queryFuture
        // regardless of the willUpdate value, since there can be an update in flight
        // with willUpdate set to false.  was needed to make testLiveQueryStop() unit test pass.
        if (queryFuture != null) {
            boolean cancelled = queryFuture.cancel(true);
            Log.v(Log.TAG_QUERY, "%s: cancelled queryFuture %s, returned: %s", this, queryFuture, cancelled);
        }

        if (rerunUpdateFuture != null) {
            boolean cancelled = rerunUpdateFuture.cancel(true);
            Log.d(Log.TAG_QUERY, "%s: cancelled rerunUpdateFuture %s, returned: %s", this, rerunUpdateFuture, cancelled);
        }

    }

    /**
     * Blocks until the intial async query finishes. After this call either .rows or .error will be non-nil.
     *
     * TODO: It seems that implementation of waitForRows() is not correct. Should fix this!!!
     *       https://github.com/couchbase/couchbase-lite-java-core/issues/647
     */
    @InterfaceAudience.Public
    public void waitForRows() throws CouchbaseLiteException {
        start();

        while (true) {
            try {
                queryFuture.get();
                break;
            } catch (InterruptedException e) {
                continue;
            } catch (Exception e) {
                lastError = e;
                throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
            }
        }
    }

    /**
     * Gets the results of the Query. The value will be null until the initial Query completes.
     */
    @InterfaceAudience.Public
    public QueryEnumerator getRows() {
        start();
        if (rows == null) {
            return null;
        }
        else {
            // Have to return a copy because the enumeration has to start at item #0 every time
            return new QueryEnumerator(rows);
        }
    }

    /**
     * Add a change listener to be notified when the live query result
     * set changes.
     */
    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener changeListener) {
        observers.add(changeListener);
    }

    /**
     * Remove previously added change listener
     */
    @InterfaceAudience.Public
    public void removeChangeListener(ChangeListener changeListener) {
        observers.remove(changeListener);
    }

    /**
     * The type of event raised when a LiveQuery result set changes.
     */
    @InterfaceAudience.Public
    public static class ChangeEvent {

        private LiveQuery source;
        private Throwable error;
        private QueryEnumerator queryEnumerator;

        ChangeEvent() {
        }

        ChangeEvent(LiveQuery source, QueryEnumerator queryEnumerator) {
            this.source = source;
            this.queryEnumerator = queryEnumerator;
        }

        ChangeEvent(Throwable error) {
            this.error = error;
        }

        public LiveQuery getSource() {
            return source;
        }

        public Throwable getError() {
            return error;
        }

        public QueryEnumerator getRows() {
            return queryEnumerator;
        }

    }

    /**
     * A delegate that can be used to listen for LiveQuery result set changes.
     */
    @InterfaceAudience.Public
    public static interface ChangeListener {
        public void changed(ChangeEvent event);
    }

    @InterfaceAudience.Private
    /* package */ void update() {
        Log.v(Log.TAG_QUERY, "%s: update() called.", this);

        if (runningState.get() == false) {
            Log.d(Log.TAG_QUERY, "%s: update() called, but running state == false.  Ignoring.", this);
            return;
        }

        if (queryFuture != null && !queryFuture.isCancelled() && !queryFuture.isDone()) {
            // There is a already a query in flight, so leave it alone except to schedule something
            // to run update() again once it finishes.
            Log.d(Log.TAG_QUERY, "%s: already a query in flight, scheduling call to update() once it's done", LiveQuery.this);
            if (rerunUpdateFuture != null && !rerunUpdateFuture.isCancelled() && !rerunUpdateFuture.isDone()) {
                boolean cancelResult = rerunUpdateFuture.cancel(true);
                Log.d(Log.TAG_QUERY, "%s: cancelled %s result: %s", LiveQuery.this, rerunUpdateFuture, cancelResult);
            }
            rerunUpdateFuture = rerunUpdateAfterQueryFinishes(queryFuture);
            Log.d(Log.TAG_QUERY, "%s: created new rerunUpdateFuture: %s", LiveQuery.this, rerunUpdateFuture);
            return;
        }

        // No query in flight, so kick one off
        queryFuture = runAsyncInternal(new QueryCompleteListener() {
            @Override
            public void completed(QueryEnumerator rowsParam, Throwable error) {
                if (error != null) {
                    for (ChangeListener observer : observers) {
                        observer.changed(new ChangeEvent(error));
                    }
                    lastError = error;
                } else {

                    if (runningState.get() == false) {
                        Log.d(Log.TAG_QUERY, "%s: update() finished query, but running state == false.", this);
                        return;
                    }

                    if (rowsParam != null && !rowsParam.equals(rows)) {
                        setRows(rowsParam);
                        for (ChangeListener observer : observers) {
                            Log.d(Log.TAG_QUERY, "%s: update() calling back observer with rows", LiveQuery.this);
                            // TODO: LiveQuery.ChangeListener should not be fired for non-match?
                            // https://github.com/couchbase/couchbase-lite-java-core/issues/648
                            observer.changed(new ChangeEvent(LiveQuery.this, rows));
                        }
                    }
                    lastError = null;
                }
            }
        });
        Log.d(Log.TAG_QUERY, "%s: update() created queryFuture: %s", this, queryFuture);

    }

    /**
     * kick off async task that will wait until the query finishes, and after it
     * does, it will run upate() again in case the current query in flight misses
     * some of the recently added items.
     */
    private Future rerunUpdateAfterQueryFinishes(final Future queryFutureInProgress) {
        if (queryFutureInProgress == null) {
            throw new NullPointerException();
        }
        return getDatabase().getManager().runAsync(new Runnable() {
            @Override
            public void run() {

                if (runningState.get() == false) {
                    Log.v(Log.TAG_QUERY, "%s: rerunUpdateAfterQueryFinishes.run() fired, but running state == false.  Ignoring.", this);
                    return;
                }

                while (true) {
                    try {
                        queryFutureInProgress.get();
                    } catch (InterruptedException e) {
                        continue;
                    } catch (CancellationException e) {
                        // can safely ignore these
                    } catch (ExecutionException e) {
                        Log.e(Log.TAG_QUERY, "Got exception waiting for queryFutureInProgress to finish", e);
                    }
                    break;
                }

                if (runningState.get() == false) {
                    Log.v(Log.TAG_QUERY, "%s: queryFutureInProgress.get() done, but running state == false.", this);
                    return;
                }

                update();
            }
        });
    }


    /**
     * @exclude
     */
    @Override
    @InterfaceAudience.Private
    public void changed(Database.ChangeEvent event) {
        update();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private synchronized void setRows(QueryEnumerator queryEnumerator) {
        rows = queryEnumerator;
    }

    /**
     *
     */
    @InterfaceAudience.Public
    public void queryOptionsChanged() {
        this.update();
    }
}
