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
package com.couchbase.lite.support;

import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Utility that queues up objects until the queue fills up or a time interval elapses,
 * then passes objects, in groups of its capacity, to a client-supplied processor block.
 */
public class Batcher<T> {
    ///////////////////////////////////////////////////////////////////////////
    // Constants
    ///////////////////////////////////////////////////////////////////////////

    private static long SMALL_DELAY_AFTER_LONG_PAUSE = 500; // in Milliseconds

    ///////////////////////////////////////////////////////////////////////////
    // Instance Variables
    ///////////////////////////////////////////////////////////////////////////

    private ScheduledExecutorService workExecutor;
    private int capacity = 0;
    private long delay = 0;
    private List<T> inbox = new ArrayList<T>();

    private boolean scheduled = false;
    private long scheduledDelay = 0;
    private ScheduledFuture pendingFuture = null;

    private BatchProcessor<T> processor;
    private long lastProcessedTime = 0;

    private boolean isFlushing = false;

    private final Object mutex = new Object();
    private final Object processMutex = new Object();

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Initializes a batcher.
     *
     * @param workExecutor the work executor that performs actual work
     * @param capacity     The maximum number of objects to batch up. If the queue reaches
     *                     this size, the queued objects will be sent to the processor
     *                     immediately.
     * @param delay        The maximum waiting time in milliseconds to collect objects
     *                     before processing them. In some circumstances objects will be
     *                     processed sooner.
     * @param processor    The callback/block that will be called to process the objects.
     */
    public Batcher(ScheduledExecutorService workExecutor,
                   int capacity,
                   long delay,
                   BatchProcessor<T> processor) {
        this.workExecutor = workExecutor;
        this.capacity = capacity;
        this.delay = delay;
        this.processor = processor;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Methods - Public
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get capacity amount.
     */
    public int getCapacity() {
        synchronized (mutex) {
            return capacity;
        }
    }

    /**
     * Get delay amount.
     */
    public long getDelay() {
        synchronized (mutex) {
            return delay;
        }
    }

    public boolean isEmpty() {
        synchronized (mutex) {
            return inbox.size() == 0 &&
                    (pendingFuture == null || pendingFuture.isDone() || pendingFuture.isCancelled());
        }
    }

    /**
     * The number of objects currently in the queue.
     */
    public int count() {
        synchronized (mutex) {
            return inbox.size();
        }
    }

    /**
     * Adds an object to the queue.
     */
    public void queueObject(T object) {
        queueObjects(Collections.singletonList(object));
    }

    /**
     * Adds multiple objects to the queue.
     */
    public void queueObjects(List<T> objects) {
        if (objects == null || objects.size() == 0)
            return;

        boolean readyToProcess = false;
        synchronized (mutex) {
            Log.v(Log.TAG_BATCHER, "%s: queueObjects called with %d objects (current inbox size = %d)",
                    this, objects.size(), inbox.size());
            inbox.addAll(objects);
            mutex.notifyAll();

            if (isFlushing) {
                // Skip scheduling as flushing is processing all the queue objects:
                return;
            }

            scheduleBatchProcess(false);

            if (inbox.size() >= capacity && isPendingFutureReadyOrInProcessing())
                readyToProcess = true;
        }

        if (readyToProcess) {
            // Give work executor chance to work on a scheduled task and to obtain the
            // mutex lock when another thread keeps adding objects to the queue fast:
            synchronized (processMutex) {
                try {
                    processMutex.wait(5);
                } catch (InterruptedException e) { }
            }
        }
    }

    /**
     * Sends _all_ the queued objects at once to the processor block.
     * After this method returns, all inbox objects will be processed.
     *
     * @param waitForAllToFinish wait until all objects are processed. If set to True,
     *                           need to make sure not to call flushAll in the same
     *                           WorkExecutor used by the batcher as it will result to
     *                           deadlock.
     */
    public void flushAll(boolean waitForAllToFinish) {
        Log.v(Log.TAG_BATCHER, "%s: flushing all objects (wait=%b)", this, waitForAllToFinish);

        synchronized (mutex) {
            isFlushing = true;
            unschedule();
        }

        while (true) {
            ScheduledFuture future;
            synchronized (mutex) {
                if (inbox.size() == 0)
                    break; // Nothing to do

                final List<T> toProcess = new ArrayList<T>(inbox);
                inbox.clear();
                mutex.notifyAll();

                future = workExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        processor.process(toProcess);
                        synchronized (mutex) {
                            lastProcessedTime = System.currentTimeMillis();
                        }
                    }
                }, 0, TimeUnit.MILLISECONDS);
            }

            if (waitForAllToFinish) {
                if (future != null && !future.isDone() && !future.isCancelled()) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        Log.e(Log.TAG_BATCHER, "%s: Error while waiting for pending future " +
                                "when flushing all items", e, this);
                    }
                }
            }
        }

        synchronized (mutex) {
            isFlushing = false;
        }
    }

    /**
     * Empties the queue without processing any of the objects in it.
     */
    public void clear() {
        synchronized (mutex) {
            unschedule();
            inbox.clear();
            mutex.notifyAll();
        }
    }

    /**
     * Wait for the **current** items in the queue to be all processed.
     *
     * Note: Calling this method on the same thread as the WorkExecutor set to the batcher
     * will result to deadlock.
     */
    public void waitForPendingFutures() {
        // Wait inbox to become empty:
        Log.v(Log.TAG_BATCHER, "%s: waitForPendingFutures is called ...", this);

        while (true) {
            ScheduledFuture future;
            synchronized (mutex) {
                while (!inbox.isEmpty()) {
                    try {
                        Log.v(Log.TAG_BATCHER, "%s: waitForPendingFutures, inbox size: %d",
                                this, inbox.size());
                        mutex.wait(300);
                    } catch (InterruptedException e) {}
                }
                future = pendingFuture;
            }

            // Wait till ongoing computation completes:
            if (future != null && !future.isDone() && !future.isCancelled()) {
                try {
                    future.get();
                } catch (Exception e) {
                    Log.e(Log.TAG_BATCHER, "%s: Error while waiting for pending futures", e, this);
                }
            }

            synchronized (mutex) {
                if (inbox.isEmpty())
                    break;
            }
        }

        Log.v(Log.TAG_BATCHER, "%s: waitForPendingFutures done", this);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Methods - protected or private
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Schedule batch process based on capacity, inbox size, and last processed time.
     * @param immediate flag to schedule the batch process immediately regardless.
     */
    private void scheduleBatchProcess(boolean immediate) {
        synchronized (mutex) {
            if (inbox.size() == 0)
                return;

            // Schedule the processing. To improve latency, if we haven't processed anything
            // in at least our delay time, rush these object(s) through a minimum delay:
            long suggestedDelay = 0;
            if (!immediate && inbox.size() < capacity) {
                // Check with the last processed time:
                if (System.currentTimeMillis() - lastProcessedTime < delay)
                    suggestedDelay = delay;
                else {
                    // Note: iOS schedules with 0 delay but the iOS implementation
                    // works on the runloop which still allows the current thread
                    // to continue queuing objects to the batcher until going out of
                    // the runloop. Java cannot do the same so giving a small delay to
                    // allow objects to be added to the batch if available:
                    suggestedDelay = Math.min(SMALL_DELAY_AFTER_LONG_PAUSE, delay);
                }
            }
            scheduleWithDelay(suggestedDelay);
        }
    }

    /**
     * Schedule the batch processing with the delay. If there is one batch currently
     * in processing, the schedule will be ignored as after the processing is done,
     * the next batch will be rescheduled.
     * @param delay delay to schedule the work executor to process the next batch.
     */
    private void scheduleWithDelay(long delay) {
        synchronized (mutex) {
            if (scheduled && delay < scheduledDelay) {
                if (isPendingFutureReadyOrInProcessing()) {
                    // Ignore as there is one batch currently in processing or ready to be processed:
                    Log.v(Log.TAG_BATCHER, "%s: scheduleWithDelay: %d ms, ignored as current batch " +
                            "is ready or in process", this, delay);
                    return;
                }
                unschedule();
            }

            if (!scheduled) {
                scheduled = true;
                scheduledDelay = delay;
                Log.v(Log.TAG_BATCHER, "%s: scheduleWithDelay %d ms, scheduled ...", this, delay);
                pendingFuture = workExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        Log.v(Log.TAG_BATCHER, "%s: call processNow ...", this);
                        processNow();
                        Log.v(Log.TAG_BATCHER, "%s: call processNow done", this);
                    }
                }, scheduledDelay, TimeUnit.MILLISECONDS);
            } else
                Log.v(Log.TAG_BATCHER, "%s: scheduleWithDelay %d ms, ignored", this, delay);
        }
    }

    /**
     * Unschedule the scheduled batch processing.
     */
    private void unschedule() {
        synchronized (mutex) {
            if (pendingFuture != null && !pendingFuture.isDone() && !pendingFuture.isCancelled()) {
                Log.v(Log.TAG_BATCHER, "%s: cancelling the pending future ...", this);
                pendingFuture.cancel(false);
            }
            scheduled = false;
        }
    }

    /**
     * Check if the current pending future is ready to be processed or in processing.
     * @return true if the current pending future is ready to be processed or in processing.
     * Otherwise false. Will also return false if the current pending future is done or cancelled.
     */
    private boolean isPendingFutureReadyOrInProcessing() {
        synchronized (mutex) {
            if (pendingFuture != null && !pendingFuture.isDone() && !pendingFuture.isCancelled()) {
                return pendingFuture.getDelay(TimeUnit.MILLISECONDS) <= 0;
            }
            return false;
        }
    }

    /**
     * This method is called by the work executor to do the batch process.
     * The inbox items up to the batcher capacity will be taken out to process.
     * The next batch will be rescheduled if there are still some items left in the
     * inbox.
     */
    private void processNow() {
        List<T> toProcess;
        boolean scheduleNextBatchImmediately = false;
        synchronized (mutex) {
            int count = inbox.size();
            Log.v(Log.TAG_BATCHER, "%s: processNow() called, inbox size: %d", this, count);
            if (count == 0)
                return;
            else if (count <= capacity) {
                toProcess = new ArrayList<T>(inbox);
                inbox.clear();
            } else {
                toProcess = new ArrayList<T>(inbox.subList(0, capacity));
                for (int i = 0; i < capacity; i++)
                    inbox.remove(0);
                scheduleNextBatchImmediately = true;
            }
            mutex.notifyAll();
        }

        synchronized (processMutex) {
            if (toProcess != null && toProcess.size() > 0) {
                Log.v(Log.TAG_BATCHER, "%s: invoking processor %s with %d items",
                        this, processor, toProcess.size());
                processor.process(toProcess);
            } else
                Log.v(Log.TAG_BATCHER, "%s: nothing to process", this);

            synchronized (mutex) {
                lastProcessedTime = System.currentTimeMillis();
                scheduled = false;
                scheduleBatchProcess(scheduleNextBatchImmediately);
                Log.v(Log.TAG_BATCHER, "%s: invoking processor done",
                        this, processor, toProcess.size());
            }
            processMutex.notifyAll();
        }
    }
}
