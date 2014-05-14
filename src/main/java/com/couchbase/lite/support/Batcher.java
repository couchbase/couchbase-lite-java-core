package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.util.Log;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Utility that queues up objects until the queue fills up or a time interval elapses,
 * then passes objects, in groups of its capacity, to a client-supplied processor block.
 */
public class Batcher<T> {

    private ScheduledExecutorService workExecutor;
    private ScheduledFuture<?> flushFuture;
    private int capacity;
    private int delay;
    private int scheduledDelay;
    private LinkedHashSet<T> inbox;
    private BatchProcessor<T> processor;
    private boolean scheduled = false;
    private long lastProcessedTime;

    private Runnable processNowRunnable = new Runnable() {

        @Override
        public void run() {
            try {
                processNow();
            } catch (Exception e) {
                // we don't want this to crash the batcher
                com.couchbase.lite.util.Log.e(Log.TAG_SYNC, this + ": BatchProcessor throw exception", e);
            }
        }
    };


    /**
     * Initializes a batcher.
     *
     * @param workExecutor the work executor that performs actual work
     * @param capacity The maximum number of objects to batch up. If the queue reaches this size, the queued objects will be sent to the processor immediately.
     * @param delay The maximum waiting time to collect objects before processing them. In some circumstances objects will be processed sooner.
     * @param processor The callback/block that will be called to process the objects.
     */
    public Batcher(ScheduledExecutorService workExecutor, int capacity, int delay, BatchProcessor<T> processor) {
        this.workExecutor = workExecutor;
        this.capacity = capacity;
        this.delay = delay;
        this.processor = processor;
    }

    /**
     * Adds multiple objects to the queue.
     */
    public synchronized void queueObjects(List<T> objects) {

        Log.v(Log.TAG_SYNC, "%s: queueObjects called with %d objects. ", this, objects.size());
        if (objects.size() == 0) {
            return;
        }
        if (inbox == null) {
            inbox = new LinkedHashSet<T>();
        }

        Log.v(Log.TAG_SYNC, "%s: inbox size before adding objects: %d", this, inbox.size());

        inbox.addAll(objects);

        scheduleWithDelay(delayToUse());
    }

    /**
     * Adds an object to the queue.
     */
    public void queueObject(T object) {
        List<T> objects = Arrays.asList(object);
        queueObjects(objects);
    }

    /**
     * Sends queued objects to the processor block (up to the capacity).
     */
    public void flush() {
        scheduleWithDelay(delayToUse());
    }

    /**
     * Sends _all_ the queued objects at once to the processor block.
     * After this method returns, the queue is guaranteed to be empty.
     */
    public void flushAll() {
        while (inbox.size() > 0) {
            unschedule();
            List<T> toProcess = new ArrayList<T>();
            toProcess.addAll(inbox);
            processor.process(toProcess);
            lastProcessedTime = System.currentTimeMillis();
        }
    }

    /**
     * Empties the queue without processing any of the objects in it.
     */
    public void clear() {
        Log.v(Log.TAG_SYNC, "%s: clear() called, setting inbox to null", this);
        unschedule();
        inbox = null;
    }

    public int count() {
        synchronized(this) {
            if(inbox == null) {
                return 0;
            }
            return inbox.size();
        }
    }

    private void processNow() {

        Log.v(Log.TAG_SYNC, this + ": processNow() called");

        scheduled = false;
        List<T> toProcess = new ArrayList<T>();

        synchronized (this) {
            if (inbox == null || inbox.size() == 0) {
                Log.v(Log.TAG_SYNC, this + ": processNow() called, but inbox is empty");
                return;
            } else if (inbox.size() <= capacity) {
                Log.v(Log.TAG_SYNC, "%s: inbox.size() <= capacity, adding %d items from inbox -> toProcess", this, inbox.size());
                toProcess.addAll(inbox);
                inbox = null;
            } else {
                Log.v(Log.TAG_SYNC, "%s: processNow() called, inbox size: %d", this, inbox.size());
                int i = 0;
                for (T item: inbox) {
                    toProcess.add(item);
                    i += 1;
                    if (i >= capacity) {
                        break;
                    }
                }

                for (T item : toProcess) {
                    Log.v(Log.TAG_SYNC, "%s: processNow() removing %s from inbox", this, item);
                    inbox.remove(item);
                }

                Log.v(Log.TAG_SYNC, "%s: inbox.size() > capacity, moving %d items from inbox -> toProcess array", this, toProcess.size());

                // There are more objects left, so schedule them Real Soon:
                scheduleWithDelay(delayToUse());

            }

        }
        if(toProcess != null && toProcess.size() > 0) {
            Log.v(Log.TAG_SYNC, "%s: invoking processor with %d items ", this, toProcess.size());
            processor.process(toProcess);
        } else {
            Log.v(Log.TAG_SYNC, "%s: nothing to process", this);
        }
        lastProcessedTime = System.currentTimeMillis();

    }

    private void scheduleWithDelay(int suggestedDelay) {
        Log.v(Log.TAG_SYNC, "%s: scheduleWithDelay called with delay: %d ms", this, suggestedDelay);
        if (scheduled && (suggestedDelay < scheduledDelay)) {
            Log.v(Log.TAG_SYNC, "%s: already scheduled and: %d < %d --> unscheduling", this, suggestedDelay, scheduledDelay);
            unschedule();
        }
        if (!scheduled) {
            Log.v(Log.TAG_SYNC, "not already scheduled");
            scheduled = true;
            scheduledDelay = suggestedDelay;
            Log.v(Log.TAG_SYNC, "workExecutor.schedule() with delay: %d ms", suggestedDelay);
            flushFuture = workExecutor.schedule(processNowRunnable, suggestedDelay, TimeUnit.MILLISECONDS);
        }
    }

    private void unschedule() {
        Log.v(Log.TAG_SYNC, this + ": unschedule() called");
        scheduled = false;
        if(flushFuture != null) {
            boolean didCancel = flushFuture.cancel(false);
            Log.v(Log.TAG_SYNC, "tried to cancel flushFuture, result: %s", didCancel);

        } else {
            Log.v(Log.TAG_SYNC, "flushFuture was null, doing nothing");
        }
    }

    private int delayToUse() {
        int delayToUse = delay;
        long delta = (System.currentTimeMillis() - lastProcessedTime);
        if (delta >= delay) {
            delayToUse = 0;
        }
        return delayToUse;
    }
}
