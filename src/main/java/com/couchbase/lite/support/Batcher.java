package com.couchbase.lite.support;

import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.Arrays;
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
    // Instance Variables
    ///////////////////////////////////////////////////////////////////////////

    private ScheduledExecutorService workExecutor;

    private int capacity = 0;
    private int delay = 0;
    private List<T> inbox;
    private boolean scheduled = false;
    private int scheduledDelay = 0;
    private BatchProcessor<T> processor;
    private long lastProcessedTime = 0;
    private ScheduledFuture pendingFuture = null;

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Initializes a batcher.
     *
     * @param workExecutor the work executor that performs actual work
     * @param capacity     The maximum number of objects to batch up. If the queue reaches this size, the queued objects will be sent to the processor immediately.
     * @param delay        The maximum waiting time in milliseconds to collect objects before processing them. In some circumstances objects will be processed sooner.
     * @param processor    The callback/block that will be called to process the objects.
     */
    public Batcher(ScheduledExecutorService workExecutor,
                   int capacity,
                   int delay,
                   BatchProcessor<T> processor) {
        this.workExecutor = workExecutor;
        this.capacity = capacity;
        this.delay = delay;
        this.processor = processor;
        this.inbox = Collections.synchronizedList(new ArrayList<T>());
        this.scheduled = false;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Methods - Public
    ///////////////////////////////////////////////////////////////////////////


    public int getCapacity() {
        return capacity;
    }

    public int getDelay() {
        return delay;
    }

    /**
     * The number of objects currently in the queue.
     */
    public int count() {
        synchronized (inbox) {
            return inbox.size();
        }
    }

    /**
     * Adds an object to the queue.
     */
    public void queueObject(T object) {
        List<T> objects = Arrays.asList(object);
        queueObjects(objects);
    }

    /**
     * Adds multiple objects to the queue.
     */
    public void queueObjects(List<T> objects) {
        if (objects == null || objects.size() == 0)
            return;

        Log.v(Log.TAG_BATCHER, "%s: queueObjects called with %d objects. Thread: %s", this, objects.size(), Thread.currentThread());

        synchronized (inbox) {
            Log.v(Log.TAG_BATCHER, "%s: inbox size before adding objects: %d", this, inbox.size());
            inbox.addAll(objects);
            inbox.notify();

            if (inbox.size() < capacity) {
                // Schedule the processing. To improve latency, if we haven't processed anything
                // in at least our delay time, rush these object(s) through ASAP:
                int suggestedDelay = delay;
                if (System.currentTimeMillis() - lastProcessedTime >= suggestedDelay)
                    suggestedDelay = 0;
                scheduleWithDelay(suggestedDelay);
            } else {
                // If inbox fills up, process it immediately:
                unschedule();
                processNow();
            }
        }
    }

    /**
     * Sends queued objects to the processor block (up to the capacity).
     */
    public void flush() {
        unschedule();
        processNow();
    }

    /**
     * Sends _all_ the queued objects at once to the processor block.
     * After this method returns, the queue is guaranteed to be empty.
     */
    public void flushAll() {
        synchronized (inbox) {
            while (inbox.size() > 0) {
                unschedule();
                List<T> toProcess = new ArrayList<T>(inbox);
                inbox.clear();
                inbox.notify();
                processor.process(toProcess);
                lastProcessedTime = System.currentTimeMillis();
            }
        }
    }

    /**
     * Empties the queue without processing any of the objects in it.
     */
    public void clear() {
        unschedule();
        inbox.clear();
        inbox.notify();
    }

    public void waitForPendingFutures() {
        Log.v(Log.TAG_BATCHER, "%s: waitForPendingFutures", this);

        // wait till ongoing computation completes
        if (pendingFuture != null && !pendingFuture.isDone() && !pendingFuture.isCancelled()) {
            try {
                pendingFuture.get();
            } catch (Exception e) {
                Log.i(Log.TAG_BATCHER, "Exception from Future.get()");
            }
        }

        // wait inbox becomes empty
        synchronized (inbox) {
            while (!inbox.isEmpty()) {
                try {
                    inbox.wait();
                } catch (InterruptedException e) {
                }
            }
        }

        // wait till ongoing computation completes
        if (pendingFuture != null && !pendingFuture.isDone() && !pendingFuture.isCancelled()) {
            try {
                pendingFuture.get();
            } catch (Exception e) {
                Log.i(Log.TAG_BATCHER, "Exception from Future.get()");
            }
        }
        Log.v(Log.TAG_BATCHER, "%s: /waitForPendingFutures", this);
    }


    ///////////////////////////////////////////////////////////////////////////
    // Instance Methods - protected or private
    ///////////////////////////////////////////////////////////////////////////

    private void unschedule() {
        scheduled = false;
        // cancel
        if (pendingFuture != null && !pendingFuture.isDone() && !pendingFuture.isCancelled())
            pendingFuture.cancel(false);
    }

    private void processNow() {
        synchronized (inbox) {
            Log.v(Log.TAG_BATCHER, this + ": processNow() called " + inbox.size());
            scheduled = false;
            List<T> toProcess;
            int count = inbox.size();
            if (count == 0) {
                Log.v(Log.TAG_BATCHER, this + ": processNow() called, but inbox is empty");
                return;
            } else if (count <= capacity) {
                toProcess = new ArrayList<T>(inbox);
                inbox.clear();
                inbox.notify();
            } else {
                toProcess = new ArrayList<T>(inbox.subList(0, capacity));
                for (int i = 0; i < capacity; i++)
                    inbox.remove(0);
                inbox.notify();
                // There are more objects left, so schedule them Real Soon:
                scheduleWithDelay(0);
            }

            if (toProcess != null && toProcess.size() > 0) {
                Log.v(Log.TAG_BATCHER, "%s: invoking processor %s with %d items ", this, processor, toProcess.size());
                processor.process(toProcess);

            } else {
                Log.v(Log.TAG_BATCHER, "%s: nothing to process", this);
            }
            lastProcessedTime = System.currentTimeMillis();
        }
    }

    private void scheduleWithDelay(int delay) {
        Log.v(Log.TAG_BATCHER, "scheduleWithDelay: " + delay + ", scheduled:" + scheduled);
        if (scheduled && delay < scheduledDelay)
            unschedule();
        if (!scheduled) {
            scheduled = true;
            scheduledDelay = delay;
            pendingFuture = workExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    processNow();
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
    }
}
