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
package com.couchbase.lite.replicator;

import com.couchbase.lite.Database;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.support.CustomFuture;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.CancellableRunnable;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import okhttp3.Response;

/**
 * Wraps a RemoteRequest with the ability to retry the request
 * <p/>
 * Huge caveat: this cannot work on a single threaded requestExecutor,
 * since it blocks while the "subrequests" are in progress, and during the sleeps
 * in between the retries.
 */
public class RemoteRequestRetry<T> implements CustomFuture<T> {
    /**
     * The kind of RemoteRequest that will be created on each retry attempt
     */
    public enum RemoteRequestType {
        REMOTE_REQUEST,
        REMOTE_MULTIPART_REQUEST,
        REMOTE_MULTIPART_DOWNLOADER_REQUEST
    }

    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    public static final String TAG = Log.TAG_REMOTE_REQUEST;
    public static int MAX_RETRIES = 3;  // total number of attempts = 4 (1 initial + MAX_RETRIES)
    public static int RETRY_DELAY_MS = 4 * 1000; // 4 sec

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////
    protected ScheduledExecutorService workExecutor;
    protected ExecutorService requestExecutor;  // must have more than one thread
    protected final HttpClientFactory clientFactory;
    protected String method;
    protected URL url;
    protected Map<String, Object> body;
    protected Map<String, Object> attachments;
    protected Authenticator authenticator;
    protected RemoteRequestCompletion onCompletionCaller;
    protected RemoteRequestCompletion onPreCompletionCaller;
    private int retryCount;
    private Database db;
    private AtomicBoolean completed = new AtomicBoolean(false);
    private Response requestHttpResponse;
    private Object requestResult;
    private Throwable requestThrowable;
    private BlockingQueue<Future> pendingRequests;
    Map<Future, CancellableRunnable> runnables = new HashMap<Future, CancellableRunnable>();
    private boolean syncGateway = true;
    private boolean cancelable = true;
    // if true, we wont log any 404 errors (useful when getting remote checkpoint doc)
    private boolean dontLog404;
    protected Map<String, Object> requestHeaders;
    private RemoteRequestType requestType;
    // for Retry task
    ScheduledFuture retryFuture;
    private Queue queue;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    public RemoteRequestRetry(RemoteRequestType requestType,
                              ScheduledExecutorService requestExecutor,
                              ScheduledExecutorService workExecutor,
                              HttpClientFactory clientFactory,
                              String method,
                              URL url,
                              boolean syncGateway,
                              boolean cancelable,
                              Map<String, Object> body,
                              Map<String, Object> attachments,
                              Database db,
                              Map<String, Object> requestHeaders,
                              RemoteRequestCompletion onCompletionCaller) {
        this.requestType = requestType;
        this.requestExecutor = requestExecutor;
        this.clientFactory = clientFactory;
        this.method = method;
        this.url = url;
        this.syncGateway = syncGateway;
        this.cancelable = cancelable;
        this.body = body;
        this.attachments = attachments;
        this.onCompletionCaller = onCompletionCaller;
        this.workExecutor = workExecutor;
        this.requestHeaders = requestHeaders;
        this.db = db;
        this.pendingRequests = new LinkedBlockingQueue<Future>();
        Log.v(TAG, "%s: RemoteRequestRetry created, url: %s", this, url);
    }

    ////////////////////////////////////////////////////////////
    // Public Methods
    ////////////////////////////////////////////////////////////

    public CustomFuture submit() {
        return submit(false);
    }

    /**
     * @param gzip true - send gzipped request
     */
    public CustomFuture submit(boolean gzip) {
        RemoteRequest request = generateRemoteRequest();
        if (gzip)
            request.setCompressedRequest(true);
        synchronized (requestExecutor) {
            if (!requestExecutor.isShutdown()) {
                Future future = requestExecutor.submit(request);
                pendingRequests.add(future);
                runnables.put(future, request);
            }
        }
        return this;
    }

    ////////////////////////////////////////////////////////////
    // Setter / Getter methods
    ////////////////////////////////////////////////////////////


    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public void setOnPreCompletionCaller(RemoteRequestCompletion onPreCompletionCaller) {
        this.onPreCompletionCaller = onPreCompletionCaller;
    }

    public void setDontLog404(boolean dontLog404) {
        this.dontLog404 = dontLog404;
    }

    ////////////////////////////////////////////////////////////
    // Implementations of CustomFuture/Future
    ////////////////////////////////////////////////////////////
    @Override
    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (cancelable) {
            // cancel ongoing request
            for (Future request : pendingRequests) {
                if (!request.isCancelled())
                    request.cancel(mayInterruptIfRunning);
                CancellableRunnable runnable = runnables.get(request);
                if (runnable != null)
                    runnable.cancel();
            }
        }
        // If RemoteRequestRetry is canceled, make sure if retry future is also canceled.
        if (retryFuture != null && !retryFuture.isCancelled())
            retryFuture.cancel(mayInterruptIfRunning);
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        while (retryCount <= MAX_RETRIES) {
            // requestExecutor was shutdown, no more retry.
            if (requestExecutor == null || requestExecutor.isShutdown() || completed.get())
                return null;
            // Take a future from the queue
            Future future = pendingRequests.poll(500, TimeUnit.MILLISECONDS);
            if (future == null)
                continue;
            while (!future.isDone() && !future.isCancelled()) {
                try {
                    future.get(500, TimeUnit.MILLISECONDS);
                } catch (TimeoutException te) {
                    // ignore TimeoutException
                }
            }
        }
        // exhausted attempts, callback to original caller with result.  requestThrowable
        // should contain most recent error that we received.
        // onCompletionCaller.onCompletion(requestHttpResponse, requestResult, requestThrowable);
        return null;
    }

    @Override
    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

    ////////////////////////////////////////////////////////////
    // Protected or Private Methods
    ////////////////////////////////////////////////////////////
    private RemoteRequest generateRemoteRequest() {
        requestHttpResponse = null;
        requestResult = null;
        requestThrowable = null;
        RemoteRequest request;
        switch (requestType) {
            case REMOTE_MULTIPART_REQUEST:
                request = new RemoteMultipartRequest(
                        clientFactory,
                        method,
                        url,
                        syncGateway,
                        cancelable,
                        body,
                        attachments,
                        db,
                        requestHeaders,
                        onCompletionInner);
                break;
            case REMOTE_MULTIPART_DOWNLOADER_REQUEST:
                request = new RemoteMultipartDownloaderRequest(
                        clientFactory,
                        method,
                        url,
                        cancelable,
                        body,
                        db,
                        requestHeaders,
                        onCompletionInner);
                break;
            default:
                request = new RemoteRequest(
                        clientFactory,
                        method,
                        url,
                        cancelable,
                        body,
                        requestHeaders,
                        onCompletionInner);
                break;
        }
        request.setDontLog404(dontLog404);
        if (this.authenticator != null)
            request.setAuthenticator(this.authenticator);
        if (this.onPreCompletionCaller != null)
            request.setOnPreCompletion(this.onPreCompletionCaller);
        return request;
    }

    private void removeFromQueue() {
        if (queue != null) {
            queue.remove(this);
            setQueue(null);
        }
    }

    private RemoteRequestCompletion onCompletionInner = new RemoteRequestCompletion() {
        @Override
        public void onCompletion(Response response, Object result, Throwable e) {
            Log.d(TAG, "%s: RemoteRequestRetry inner request finished, url: %s", this, url);
            if (e == null) {
                Log.d(TAG, "%s: RemoteRequestRetry was successful, calling callback url: %s", this, url);
                // just propagate completion block call back to the original caller
                completed(response, result, e);
            } else {
                // Only retry if error is  TransientError (5xx).
                if (isTransientError(response, e)) {
                    if (requestExecutor != null && requestExecutor.isShutdown()) {
                        // requestExecutor was shutdown, no more retry.
                        Log.e(TAG, "%s: RemoteRequestRetry failed, RequestExecutor was shutdown. url: %s", this, url);
                        completed(response, result, e);
                    } else if (retryCount >= MAX_RETRIES) {
                        Log.d(TAG, "%s: RemoteRequestRetry failed, but transient error.  retries exhausted. url: %s", this, url);
                        // ok, we're out of retries, propagate completion block call
                        completed(response, result, e);
                    } else {
                        // we're going to try again, so don't call the original caller's
                        // completion block yet.  Eventually it will get called though
                        Log.d(TAG, "%s: RemoteRequestRetry failed, but transient error.  will retry. url: %s", this, url);
                        requestHttpResponse = response;
                        requestResult = result;
                        requestThrowable = e;
                        retryCount += 1;
                        // delay * 2 << retry
                        long delay = RETRY_DELAY_MS * (long) Math.pow((double) 2, (double) Math.min(retryCount - 1, MAX_RETRIES));
                        retryFuture = workExecutor.schedule(new Runnable() {
                            @Override
                            public void run() {
                                submit();
                            }
                        }, delay, TimeUnit.MILLISECONDS); // delay init_delay * 2^retry ms
                    }
                } else {
                    Log.d(TAG, "%s: RemoteRequestRetry failed, non-transient error.  NOT retrying. url: %s", this, url);
                    // this isn't a transient error, so there's no point in retrying
                    completed(response, result, e);
                }
            }
        }

        private void completed(Response response, Object result, Throwable e) {
            requestHttpResponse = response;
            requestResult = result;
            requestThrowable = e;
            onCompletionCaller.onCompletion(requestHttpResponse, requestResult, requestThrowable);
            // release unnecessary references to reduce memory usage as soon as called onComplete().
            requestHttpResponse = null;
            requestResult = null;
            requestThrowable = null;
            removeFromQueue();
            completed.set(true);
        }
    };

    private boolean isTransientError(Response response, Throwable e) {
        Log.d(TAG, "%s: isTransientError, httpResponse: %s e: %s", this, response, e);
        if (response != null) {
            Log.d(TAG, "%s: isTransientError, status code: %d",
                    this, response.code());
            if (Utils.isTransientError(response)) {
                Log.d(TAG, "%s: isTransientError, detect a transient error", this);
                return true;
            }
        }
        if (response == null || response.code() < 400) {
            if (e instanceof IOException) {
                Log.d(TAG, "%s: isTransientError, detect an IOException which is a transient error", this);
                return true;
            }
        }
        Log.d(TAG, "%s: isTransientError, return false", this);
        return false;
    }
}
