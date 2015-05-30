package com.couchbase.lite.support;

import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Utility that queues up objects until the queue fills up or a time interval elapses,
 * then passes objects, in groups of its capacity, to a client-supplied processor block.
 */
public class Batcher<T> {

    private ScheduledExecutorService workExecutor;

    private int capacity;
    private int delayMs;
    private BlockingQueue<T> inbox;
    private BatchProcessor<T> processor;
    private long lastProcessedTime = 0;
    private BlockingQueue<ScheduledFuture> pendingFutures;
    private Lock lock = new ReentrantLock();

    private Runnable processNowRunnable = new Runnable() {
        @Override
        public void run() {
            lock.lock();
            try {
                processNow();
            } catch (Exception e) {
                // we don't want this to crash the batcher
                Log.e(Log.TAG_BATCHER, this + ": BatchProcessor throw exception", e);
            } finally {
                lock.unlock();
            }
        }
    };

    /**
     * Initializes a batcher.
     *
     * @param workExecutor the work executor that performs actual work
     * @param capacity The maximum number of objects to batch up. If the queue reaches this size, the queued objects will be sent to the processor immediately.
     * @param delayMs The maximum waiting time in milliseconds to collect objects before processing them. In some circumstances objects will be processed sooner.
     * @param processor The callback/block that will be called to process the objects.
     */
    public Batcher(ScheduledExecutorService workExecutor, int capacity, int delayMs, BatchProcessor<T> processor) {
        this.workExecutor = workExecutor;
        this.capacity = capacity;
        this.delayMs = delayMs;
        this.processor = processor;
        this.inbox = new LinkedBlockingQueue<T>();
        this.pendingFutures = new LinkedBlockingQueue<ScheduledFuture>();
    }

    private boolean isCurrentlyProcessing() {
        // we'll only get the lock if we ARENT currently processing
        boolean processingNotInProgress = lock.tryLock();
        if (processingNotInProgress) {
            // we don't actually need this lock, so unlock it immediately
            lock.unlock();
        }
        return !processingNotInProgress;
    }

    /**
     * Adds multiple objects to the queue.
     */
    public void queueObjects(List<T> objects) {

        synchronized (inbox) {
            Log.v(Log.TAG_BATCHER, "%s: queueObjects called with %d objects. Thread: %s", this, objects.size(), Thread.currentThread());
            if (objects.size() == 0) {
                return;
            }

            Log.v(Log.TAG_BATCHER, "%s: inbox size before adding objects: %d", this, inbox.size());

            inbox.addAll(objects);

            if (inbox.size() >= capacity) {
                Log.v(Log.TAG_BATCHER, "%s: calling scheduleWithDelay(0)", this);
                if (!isCurrentlyProcessing()) {
                    // we only want to unschedule and re-schedule immediately if we
                    // aren't currently processing, otherwise we'll end up in a situation where
                    // the things currently processed get interrupted and re-scheduled,
                    // and nothing ever gets done.  at the end of the processNow() method,
                    // it will schedule tasks if more are still pending.
                    unscheduleAllPending();
                    scheduleWithDelay(0);
                }
            } else {
                int suggestedDelay = delayToUse();
                Log.v(Log.TAG_BATCHER, "%s: calling scheduleWithDelay(%d)", this, suggestedDelay);
                scheduleWithDelay(suggestedDelay);
            }
        }
    }

    public void waitForPendingFutures() {
        Log.d(Log.TAG_BATCHER, "%s: waitForPendingFutures", this);
        try {
            while (!pendingFutures.isEmpty()) {
                Future future = pendingFutures.take();
                try {
                    Log.d(Log.TAG_BATCHER, "calling future.get() on %s", future);
                    future.get();
                    Log.d(Log.TAG_BATCHER, "done calling future.get() on %s", future);
                } catch (CancellationException e) {
                    Log.i(Log.TAG_BATCHER, "Task was canceled: " + e.getMessage());
                } catch (ExecutionException e) {
                    Log.e(Log.TAG_BATCHER, "ERROR: Task aborted: " + e.getMessage());
                } catch (InterruptedException e) {
                    Log.w(Log.TAG_BATCHER, e.getMessage());
                }
            }
        } catch (Exception e) {
            Log.e(Log.TAG_BATCHER, "Exception waiting for pending futures: %s", e);
        }
        Log.d(Log.TAG_BATCHER, "%s: /waitForPendingFutures", this);
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

    public int count() {
        synchronized (inbox) {
            return inbox.size();
        }
    }

    /**
     * this is mainly for debugging
     */
    public int sizeOfPendingFutures() {
        synchronized (pendingFutures) {
            return pendingFutures.size();
        }
    }

    private void processNow() {

        synchronized (inbox) {
            Log.v(Log.TAG_BATCHER, this + ": processNow() called");

            List<T> toProcess = new ArrayList<T>();

            if (inbox.size() == 0) {
                Log.v(Log.TAG_BATCHER, this + ": processNow() called, but inbox is empty");
                return;
            } else if (inbox.size() <= capacity) {
                Log.v(Log.TAG_BATCHER, "%s: inbox.size() <= capacity, adding %d items from inbox -> toProcess", this, inbox.size());
                while (inbox.size() > 0) {
                    try {
                        T t = inbox.take();
                        toProcess.add(t);
                    } catch (InterruptedException e) {
                        Log.w(Log.TAG_BATCHER, "%s: processNow(): %s", this, e.getMessage());
                    }
                }
            } else {
                Log.v(Log.TAG_BATCHER, "%s: processNow() called, inbox size: %d", this, inbox.size());
                int i = 0;
                while (inbox.size() > 0 && i < capacity) {
                    try {
                        T t = inbox.take();
                        toProcess.add(t);
                    } catch (InterruptedException e) {
                        Log.w(Log.TAG_BATCHER, "%s: processNow(): %s", this, e.getMessage());
                    }
                    i += 1;
                }

                Log.v(Log.TAG_BATCHER, "%s: inbox.size() > capacity, moving %d items from inbox -> toProcess array", this, toProcess.size());
            }

            if (toProcess != null && toProcess.size() > 0) {
                Log.v(Log.TAG_BATCHER, "%s: invoking processor %s with %d items ", this, processor, toProcess.size());
                processor.process(toProcess);
            } else {
                Log.v(Log.TAG_BATCHER, "%s: nothing to process", this);
            }
            lastProcessedTime = System.currentTimeMillis();

            // in case we ignored any schedule requests while processing, if
            // we have more items in inbox, lets schedule another processing attempt
            if (inbox.size() > 0) {
                Log.v(Log.TAG_BATCHER, "%s: finished processing a batch, but inbox size > 0: %d", this, inbox.size());
                //int delayToUse = delayToUse();
                int delayToUse = 0;
                Log.v(Log.TAG_BATCHER, "%s: going to process with delay: %d", this, delayToUse);
                ScheduledFuture pendingFuture = workExecutor.schedule(processNowRunnable, delayToUse, TimeUnit.MILLISECONDS);
                pendingFutures.add(pendingFuture);
            }
        }
    }

    private void scheduleWithDelay(int suggestedDelay) {

        // keep a list of expired pending futures so we can remove them from pendingFutures
        List<ScheduledFuture> futuresToForget = new ArrayList<ScheduledFuture>();

        // do we already have anything scheduled?  if so, ignore this call to scheduleWithDelay()
        Iterator<ScheduledFuture> iterator = pendingFutures.iterator();
        while (iterator.hasNext()) {
            ScheduledFuture pendingFuture = iterator.next();
            // NOTE: It used to ignore if there is a pending task in the queue.
            //       But it rarely causes the problem if a device is slow under multi-threads environment.
            //       ProcessNow() does not process anything if inbox is empty.
            //       Executing new task with empty inbox does not causes issue.
            if (pendingFuture != null && !pendingFuture.isCancelled() && !pendingFuture.isDone()) {
                Log.v(Log.TAG_BATCHER, "%s: scheduleWithDelay already has a pending task: %s.", this, pendingFuture);
            } else {
                futuresToForget.add(pendingFuture);
            }
        }
        forgetExpiredFutures(futuresToForget);

        Log.v(Log.TAG_BATCHER, "%s: scheduleWithDelay called with delayMs: %d ms", this, suggestedDelay);
        Log.v(Log.TAG_BATCHER, "workExecutor.schedule() with delayMs: %d ms", suggestedDelay);
        ScheduledFuture pendingFuture = workExecutor.schedule(processNowRunnable, suggestedDelay, TimeUnit.MILLISECONDS);
        Log.v(Log.TAG_BATCHER, "%s: created future: %s", this, pendingFuture);
        pendingFutures.add(pendingFuture);
    }

    private void forgetExpiredFutures(List<ScheduledFuture> futuresToForget) {
        // clean out expired futures we no longer care about
        for (ScheduledFuture futureToForget : futuresToForget) {
            Log.v(Log.TAG_BATCHER, "%s: forgetting about expired future: %s", this, futureToForget);
            pendingFutures.remove(futureToForget);
        }
    }

    private void unscheduleAllPending() {
        // keep a list of expired pending futures so we can remove them from pendingFutures
        List<ScheduledFuture> futuresToForget = new ArrayList<ScheduledFuture>();

        Iterator<ScheduledFuture> iterator = pendingFutures.iterator();
        while (iterator.hasNext()) {
            ScheduledFuture pendingFuture = iterator.next();
            pendingFuture.cancel(true);
            futuresToForget.add(pendingFuture);
        }

        forgetExpiredFutures(futuresToForget);
    }

    /*
     * calculates the delayMs to use when scheduling the next batch of objects to process
     * There is a balance required between clearing down the input queue as fast as possible
     * and not exhausting downstream system resources such as sockets and http response buffers
     * by processing too many batches concurrently.
     */
    public int delayToUse() {

        //initially set the delayMs to the default value for this Batcher
        int delayToUse = delayMs;

        // have we processed anything yet?  if so, check how long its been since we last
        // processed something, and if it was longer than delayMs then use a 0 delay.
        if (lastProcessedTime > 0) {

            //get the time interval since the last batch completed to the current system time
            long delta = (System.currentTimeMillis() - lastProcessedTime);

            //if the time interval is greater or equal to the default delayMs then set the
            // delayMs so that the next batch gets scheduled to process immediately
            if (delta >= delayMs) {
                delayToUse = 0;
            }

            Log.v(Log.TAG_BATCHER, "%s: delayToUse() delta: %d, delayToUse: %d, delayMs: %d", this, delta, delayToUse, delayMs);
        }
        return delayToUse;
    }
}
