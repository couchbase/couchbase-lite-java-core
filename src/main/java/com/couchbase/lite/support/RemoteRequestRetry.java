package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps a RemoteRequest with the ability to retry the request
 *
 * Huge caveat: this cannot work on a single threaded requestExecutor,
 * since it blocks while the "subrequests" are in progress, and during the sleeps
 * in between the retries.
 *
 */
public class RemoteRequestRetry<T> implements Future<T> {

    public static int MAX_RETRIES = 3;  // TODO: rename to MAX_ATTEMPTS
    public static int RETRY_DELAY_MS = 10 * 1000;

    protected ScheduledExecutorService workExecutor;
    protected ExecutorService requestExecutor;  // must have more than one thread

    protected final HttpClientFactory clientFactory;
    protected String method;
    protected URL url;
    protected Object body;
    protected Authenticator authenticator;
    protected RemoteRequestCompletionBlock onCompletionCaller;
    protected RemoteRequestCompletionBlock onPreCompletionCaller;

    private int retryCount;
    private Database db;
    protected HttpUriRequest request;

    private AtomicBoolean completedSuccessfully = new AtomicBoolean(false);
    private HttpResponse requestHttpResponse;
    private Object requestResult;
    private Throwable requestThrowable;
    private BlockingQueue<Future> pendingRequests;

    protected Map<String, Object> requestHeaders;

    public RemoteRequestRetry(ScheduledExecutorService requestExecutor,
                              ScheduledExecutorService workExecutor,
                              HttpClientFactory clientFactory,
                              String method,
                              URL url,
                              Object body,
                              Database db,
                              Map<String, Object> requestHeaders,
                              RemoteRequestCompletionBlock onCompletionCaller) {

        this.requestExecutor = requestExecutor;
        this.clientFactory = clientFactory;
        this.method = method;
        this.url = url;
        this.body = body;
        this.onCompletionCaller = onCompletionCaller;
        this.workExecutor = workExecutor;
        this.requestHeaders = requestHeaders;
        this.db = db;
        this.pendingRequests = new LinkedBlockingDeque<Future>();

        Log.v(Log.TAG_SYNC, "%s: RemoteRequestRetry created, url: %s", this, url);

    }

    public Future submit() {

        RemoteRequest request = generateRemoteRequest();

        Future future = requestExecutor.submit(request);
        pendingRequests.add(future);

        return this;

    }

    private RemoteRequest generateRemoteRequest() {

        requestHttpResponse = null;
        requestResult = null;
        requestThrowable = null;

        RemoteRequest request = new RemoteRequest(
                workExecutor,
                clientFactory,
                method,
                url,
                body,
                db,
                requestHeaders,
                onCompletionInner
        );

        if (this.authenticator != null) {
            request.setAuthenticator(this.authenticator);
        }
        if (this.onPreCompletionCaller != null) {
            request.setOnPreCompletion(this.onPreCompletionCaller);
        }
        return request;
    }




    RemoteRequestCompletionBlock onCompletionInner = new RemoteRequestCompletionBlock() {

        private void completed(HttpResponse httpResponse, Object result, Throwable e) {
            requestHttpResponse = httpResponse;
            requestResult = result;
            requestThrowable = e;
            completedSuccessfully.set(true);
            onCompletionCaller.onCompletion(requestHttpResponse, requestResult, requestThrowable);
        }

        @Override
        public void onCompletion(HttpResponse httpResponse, Object result, Throwable e) {
            Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry inner request finished, url: %s", this, url);

            if (e == null) {
                Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry was successful, calling callback url: %s", this, url);

                // just propagate completion block call back to the original caller
                completed(httpResponse, result, e);

            } else {

                if (isTransientError(httpResponse, e)) {
                    if (retryCount >= MAX_RETRIES) {
                        Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, but transient error.  retries exhausted. url: %s", this, url);
                        // ok, we're out of retries, propagate completion block call
                        completed(httpResponse, result, e);
                    } else {
                        // we're going to try again, so don't call the original caller's
                        // completion block yet.  Eventually it will get called though
                        Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, but transient error.  will retry. url: %s", this, url);
                        requestHttpResponse = httpResponse;
                        requestResult = result;
                        requestThrowable = e;

                        retryCount += 1;

                        submit();

                    }
                } else {
                    Log.d(Log.TAG_SYNC, "%s: RemoteRequestRetry failed, non-transient error.  NOT retrying. url: %s", this, url);
                    // this isn't a transient error, so there's no point in retrying
                    completed(httpResponse, result, e);
                }


            }
        }
    };

    private boolean isTransientError(HttpResponse httpResponse, Throwable e) {

        Log.d(Log.TAG_SYNC, "%s: isTransientError called, httpResponse: %s e: %s", this, httpResponse, e);

        if (httpResponse != null) {

            if (Utils.isTransientError(httpResponse.getStatusLine())) {
                Log.d(Log.TAG_SYNC, "%s: its a transient error, return true", this);
                return true;
            }

        } else {
            if (e instanceof IOException) {
                Log.d(Log.TAG_SYNC, "%s: its an ioexception, return true", this);
                return true;
            }

        }

        Log.d(Log.TAG_SYNC, "%s: return false");
        return false;

    }

    /**
     *  Set Authenticator for BASIC Authentication
     */
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public void setOnPreCompletionCaller(RemoteRequestCompletionBlock onPreCompletionCaller) {
        this.onPreCompletionCaller = onPreCompletionCaller;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
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

            // Take a future from the queue
            Future future = pendingRequests.take();

            future.get();

            if (completedSuccessfully.get() == true) {
                // we're done
                return null;
            }

            // retryCount += 1;

            // submit();

        }

        // exhausted attempts, callback to original caller with result.  requestThrowable
        // should contain most recent error that we received.
        // onCompletionCaller.onCompletion(requestHttpResponse, requestResult, requestThrowable);

        return null;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

}
