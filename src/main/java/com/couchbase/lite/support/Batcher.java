package com.couchbase.lite.support;

import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Utility that queues up objects until the queue fills up or a time interval elapses,
 * then passes objects, in groups of its capacity, to a client-supplied processor block.
 */
public class Batcher<T> {

    private ScheduledExecutorService workExecutor;
    private ScheduledFuture flushFuture;
    private BlockingQueue<ScheduledFuture> pendingFutures;

    private int capacity;
    private int delay;
    private int scheduledDelay;
    private BlockingQueue<T> inbox;
    private BatchProcessor<T> processor;
    private boolean scheduled = false;
    private long lastProcessedTime;

    private Runnable processNowRunnable = new Runnable() {

        @Override
        public void run() {
            try {
                Log.d(Log.TAG_SYNC, "processNowRunnable.run() method starting");
                processNow();
                Log.d(Log.TAG_SYNC, "processNowRunnable.run() method finished");
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
        this.pendingFutures = new LinkedBlockingQueue<ScheduledFuture>();
        this.inbox = new LinkedBlockingQueue<T>();

    }

    /**
     * Adds multiple objects to the queue.
     */
    public void queueObjects(List<T> objects) {

        Log.v(Log.TAG_SYNC, "%s: queueObjects called with %d objects. ", this, objects.size());
        if (objects.size() == 0) {
            return;
        }

        Log.v(Log.TAG_SYNC, "%s: inbox size before adding objects: %d", this, inbox.size());

        inbox.addAll(objects);

        scheduleWithDelay(delayToUse());
    }

    public void waitForPendingFutures() {

        try {
            while (!pendingFutures.isEmpty()) {
                ScheduledFuture future = pendingFutures.take();
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
     * Empties the queue without processing any of the objects in it.
     */
    public void clear() {
        Log.v(Log.TAG_SYNC, "%s: clear() called, setting inbox to null", this);
        unschedule();
        inbox.clear();
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

        if (inbox == null || inbox.size() == 0) {
            Log.v(Log.TAG_SYNC, this + ": processNow() called, but inbox is empty");
            return;
        } else if (inbox.size() <= capacity) {
            Log.v(Log.TAG_SYNC, "%s: inbox.size() <= capacity, adding %d items from inbox -> toProcess", this, inbox.size());
            while (inbox.size() > 0) {
                try {
                    T t = inbox.take();
                    toProcess.add(t);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {
            Log.v(Log.TAG_SYNC, "%s: processNow() called, inbox size: %d", this, inbox.size());
            int i = 0;
            while (inbox.size() > 0 && i < capacity) {
                try {
                    T t = inbox.take();
                    toProcess.add(t);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i += 1;
            }


            Log.v(Log.TAG_SYNC, "%s: inbox.size() > capacity, moving %d items from inbox -> toProcess array", this, toProcess.size());

            // There are more objects left, so schedule them Real Soon:
            scheduleWithDelay(delayToUse());

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
        scheduledDelay = suggestedDelay;
        Log.v(Log.TAG_SYNC, "workExecutor.schedule() with delay: %d ms", suggestedDelay);
        ScheduledFuture future = workExecutor.schedule(processNowRunnable, suggestedDelay, TimeUnit.MILLISECONDS);
        pendingFutures.add(future);
        flushFuture = future;
    }

    private void unschedule() {
        Log.v(Log.TAG_SYNC, this + ": unschedule() called");

        try {
            while (!pendingFutures.isEmpty()) {
                ScheduledFuture future = pendingFutures.take();
                Log.d(Log.TAG_SYNC, "calling future.cancel() on %s", future);
                future.cancel(true);
                Log.d(Log.TAG_SYNC, "done calling future.cancel() on %s", future);
            }

        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, "Exception waiting for pending futures: %s", e);
        }
    }

    /*
     * calculates the delay to use when scheduling the next batch of objects to process
     * There is a balance required between clearing down the input queue as fast as possible
     * and not exhausting downstream system resources such as sockets and http response buffers
     * by processing too many batches concurrently.
     */
    private int delayToUse() {

        //initially set the delay to the default value for this Batcher
        int delayToUse = delay;

        //get the time interval since the last batch completed to the current system time
        long delta = (System.currentTimeMillis() - lastProcessedTime);

        //if the time interval is greater or equal to the default delay then set the
        // delay so that the next batch gets scheduled to process immediately
        if (delta >= delay) {
            delayToUse = 0;
        }

        Log.v(Log.TAG_SYNC, "%s: delayToUse() delta: %d, delayToUse: %d, delay: %d", this, delta, delayToUse, delta);

        return delayToUse;
    }
}
