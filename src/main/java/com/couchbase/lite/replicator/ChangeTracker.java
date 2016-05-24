/**
 * Copyright (c) 2016 Couchbase, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.couchbase.lite.replicator;

import com.couchbase.lite.Manager;
import com.couchbase.lite.Status;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * Reads the continuous-mode _changes feed of a database, and sends the
 * individual change entries to its client's changeTrackerReceivedChange()
 *
 * @exclude
 */
@InterfaceAudience.Private
public class ChangeTracker implements Runnable {
    private static final int TIMEOUT_FOR_PAUSE = 5 * 1000; // 5 sec

    private static final MediaType JSON
            = MediaType.parse("application/json; charset=utf-8");

    private URL databaseURL;
    private Object lastSequenceID;
    private boolean continuous = false;  // is enclosing replication continuous?
    private Throwable error;
    private ChangeTrackerClient client;
    protected Map<String, Object> requestHeaders;
    private Authenticator authenticator;
    private boolean usePOST;
    private boolean activeOnly = false;

    private ChangeTrackerMode mode;
    private String filterName;
    private Map<String, Object> filterParams;
    private int limit;
    private int heartBeatSeconds;
    private List<String> docIDs;

    private boolean paused = false;
    private final Object pausedObj = new Object();

    private boolean includeConflicts;
    private Thread thread;
    private boolean running = false;
    private Request request;
    private Call call;
    private InputStream inputStream = null;
    protected ChangeTrackerBackoff backoff;
    private long startTime = 0;
    private String str = null;
    private boolean caughtUp = false;

    public enum ChangeTrackerMode {
        OneShot,
        LongPoll,
        Continuous  // does not work, do not use it.
    }

    public ChangeTracker(URL databaseURL, ChangeTrackerMode mode, boolean includeConflicts,
                         Object lastSequenceID, ChangeTrackerClient client) {
        this.databaseURL = databaseURL;
        this.mode = mode;
        this.includeConflicts = includeConflicts;
        this.lastSequenceID = lastSequenceID;
        this.client = client;
        this.requestHeaders = new HashMap<String, Object>();
        this.heartBeatSeconds = Replication.DEFAULT_HEARTBEAT;
        this.limit = 50;
        this.usePOST = true;
    }

    public boolean isContinuous() {
        return continuous;
    }

    public void setContinuous(boolean continuous) {
        this.continuous = continuous;
    }

    public void setFilterName(String filterName) {
        this.filterName = filterName;
    }

    public void setFilterParams(Map<String, Object> filterParams) {
        this.filterParams = filterParams;
    }

    public void setClient(ChangeTrackerClient client) {
        this.client = client;
    }

    public boolean isActiveOnly() {
        return activeOnly;
    }

    public void setActiveOnly(boolean activeOnly) {
        this.activeOnly = activeOnly;
    }

    public String getFeed() {
        switch (mode) {
            case OneShot:
                return "normal";
            case LongPoll:
                return "longpoll";
            case Continuous:
                return "continuous";
        }
        return "normal";
    }

    public long getHeartbeatMilliseconds() {
        return heartBeatSeconds * 1000;
    }

    /**
     * - (NSString*) changesFeedPath
     * in CBLChangeTracker.m
     */
    /* package */ String getChangesFeedPath() {
        // We add the basic query params to the URL even if we'll send a POST request. Yes, this is
        // redundant, since those params are in the JSON body too. This is for CouchDB compatibility:
        // for some reason it still expects most of the params in the URL, even with a POST; only the
        // filter-related params go in the body.
        // (See https://github.com/couchbase/couchbase-lite-ios/issues/1139)
        StringBuilder sb = new StringBuilder(String.format("_changes?feed=%s&heartbeat=%d", getFeed(), getHeartbeatMilliseconds()));
        if (includeConflicts)
            sb.append("&style=all_docs");
        Object seq = lastSequenceID;
        if (seq != null) {
            if (seq instanceof List || seq instanceof Map) {
                try {
                    seq = Manager.getObjectMapper().writeValueAsString(seq);
                } catch (JsonProcessingException e) {
                }
            }
            sb.append("&since=");
            sb.append(URLEncoder.encode(seq.toString()));
        }
        if (activeOnly && !caughtUp)
            // On first replication we can skip getting deleted docs. (SG enhancement in ver. 1.2)
            sb.append("&active_only=true");
        if (limit > 0) {
            sb.append("&limit=");
            sb.append(limit);
        }

        if (!usePOST) {
            // Add filter or doc_ids to URL. If sending a POST, these will go in the JSON body instead.
            if (docIDs != null && docIDs.size() > 0) {
                filterName = "_doc_ids";
                filterParams = new HashMap<String, Object>();
                filterParams.put("doc_ids", docIDs);
            }
            if (filterName != null) {
                sb.append("&filter=");
                sb.append(URLEncoder.encode(filterName));
                if (filterParams != null) {
                    for (String key : filterParams.keySet()) {
                        Object value = filterParams.get(key);
                        if (!(value instanceof String)) {
                            try {
                                value = Manager.getObjectMapper().writeValueAsString(value);
                            } catch (JsonProcessingException e) {
                                throw new IllegalArgumentException(e);
                            }
                        }
                        sb.append("&");
                        sb.append(URLEncoder.encode(key));
                        sb.append("=");
                        sb.append(URLEncoder.encode(value.toString()));
                    }
                }
            }
        }

        return sb.toString();
    }

    public URL getChangesFeedURL() {
        String dbURLString = databaseURL.toExternalForm();
        if (!dbURLString.endsWith("/")) {
            dbURLString += "/";
        }
        dbURLString += getChangesFeedPath();
        URL result = null;
        try {
            result = new URL(dbURLString);
        } catch (MalformedURLException e) {
            Log.e(Log.TAG_CHANGE_TRACKER, this + ": Changes feed ULR is malformed", e);
        }
        return result;
    }

    /**
     * Set Authenticator for BASIC Authentication
     */
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public void run() {
        Log.d(Log.TAG_CHANGE_TRACKER, "Thread id => " + Thread.currentThread().getId());
        try {
            runLoop();
        } finally {
            // stopped() method should be called at end of run() method.
            stopped();
        }
    }

    private boolean isResponseFailed(Response response) {
        //StatusLine status = response.getStatusLine();
        if (response.code() >= 300 &&
                ((mode == ChangeTrackerMode.LongPoll && !Utils.isTransientError(response.code())) ||
                        mode != ChangeTrackerMode.LongPoll)) {
            Log.w(Log.TAG_CHANGE_TRACKER, "%s: Change tracker got error %d",
                    this, response.code());
            error = new RemoteRequestResponseException(response.code(), response.message());
            return true;
        }
        return false;
    }

    private boolean retryIfFailedPost(Response response) {
        if (!usePOST)
            return false;
        if (response.code() != Status.METHOD_NOT_ALLOWED)
            return false;
        usePOST = false;
        return true;
    }

    protected void runLoop() {
        paused = false;

        if (client == null) {
            // This is a race condition that can be reproduced by calling cbpuller.start() and cbpuller.stop()
            // directly afterwards.  What happens is that by the time the Changetracker thread fires up,
            // the cbpuller has already set this.client to null.  See issue #109
            Log.w(Log.TAG_CHANGE_TRACKER, "%s: ChangeTracker run() loop aborting because client == null", this);
            return;
        }

        if (mode == ChangeTrackerMode.Continuous) {
            // there is a failing unit test for this, and from looking at the code the Replication
            // object will never use Continuous mode anyway.  Explicitly prevent its use until
            // it is demonstrated to actually work.
            throw new RuntimeException("ChangeTracker does not correctly support continuous mode");
        }

        OkHttpClient httpClient = client.getOkHttpClient();

        backoff = new ChangeTrackerBackoff();

        while (running) {
            startTime = System.currentTimeMillis();

            Request.Builder builder = new Request.Builder();
            URL url = getChangesFeedURL();
            builder.url(url);
            if (usePOST) {
                builder.header("Content-Type", "application/json")
                        .addHeader("User-Agent", Manager.getUserAgent())
                        .addHeader("Accept-Encoding", "gzip")
                        .post(RequestBody.create(JSON, changesFeedPOSTBody()));
            }
            addRequestHeaders(builder);

            // Perform BASIC Authentication if needed
            builder = RequestUtils.preemptivelySetAuthCredentials(builder, url, authenticator);
            request = builder.build();

            try {
                String maskedRemoteWithoutCredentials = getChangesFeedURL().toString();
                maskedRemoteWithoutCredentials = maskedRemoteWithoutCredentials.replaceAll("://.*:.*@", "://---:---@");
                Log.v(Log.TAG_CHANGE_TRACKER, "%s: Making request to %s", this, maskedRemoteWithoutCredentials);
                call = httpClient.newCall(request);
                Response response = call.execute();
                try {
                    // In case response status is Error, ChangeTracker stops here
                    if (isResponseFailed(response)) {
                        if (retryIfFailedPost(response)) {
                            RequestUtils.closeResponseBody(response);
                            continue;
                        }
                        RequestUtils.closeResponseBody(response);
                        break;
                    }

                    // Parse response body
                    ResponseBody responseBody = response.body();

                    Log.v(Log.TAG_CHANGE_TRACKER, "%s: got response. status: %s mode: %s", this, response.message(), mode);
                    if (responseBody != null) {
                        try {
                            Log.v(Log.TAG_CHANGE_TRACKER, "%s: /entity.getContent().  mode: %s", this, mode);
                            //inputStream = entity.getContent();
                            inputStream = responseBody.byteStream();
                            // decompress if contentEncoding is gzip
                            if (Utils.isGzip(response))
                                inputStream = new GZIPInputStream(inputStream);

                            if (mode == ChangeTrackerMode.LongPoll) {  // continuous replications
                                // NOTE: 1. check content length, ObjectMapper().readValue() throws Exception if size is 0.
                                // NOTE: 2. HttpEntity.getContentLength() returns the number of bytes of the content, or a negative number if unknown.
                                // NOTE: 3. If Http Status is error, not parse response body
                                boolean responseOK = false; // default value
                                if (responseBody.contentLength() != 0 && response.code() < 300) {
                                    try {
                                        Log.v(Log.TAG_CHANGE_TRACKER, "%s: readValue", this);
                                        Map<String, Object> fullBody = Manager.getObjectMapper().readValue(inputStream, Map.class);
                                        Log.v(Log.TAG_CHANGE_TRACKER, "%s: /readValue.  fullBody: %s", this, fullBody);
                                        responseOK = receivedPollResponse(fullBody);
                                    } catch (JsonParseException jpe) {
                                        Log.w(Log.TAG_CHANGE_TRACKER, "%s: json parsing error; %s", this, jpe.toString());
                                    } catch (JsonMappingException jme) {
                                        Log.w(Log.TAG_CHANGE_TRACKER, "%s: json mapping error; %s", this, jme.toString());
                                    }
                                }
                                Log.v(Log.TAG_CHANGE_TRACKER, "%s: responseOK: %s", this, responseOK);

                                if (responseOK) {
                                    // TODO: this logic is questionable, there's lots
                                    // TODO: of differences in the iOS changetracker code,
                                    if (!caughtUp) {
                                        caughtUp = true;
                                        client.changeTrackerCaughtUp();
                                    }
                                    Log.v(Log.TAG_CHANGE_TRACKER, "%s: Starting new longpoll", this);
                                    backoff.resetBackoff();
                                    continue;
                                } else {
                                    long elapsed = (System.currentTimeMillis() - startTime) / 1000;
                                    Log.w(Log.TAG_CHANGE_TRACKER, "%s: Longpoll connection closed (by proxy?) after %d sec", this, elapsed);
                                    if (elapsed >= 30) {
                                        // Looks like the connection got closed by a proxy (like AWS' load balancer) while the
                                        // server was waiting for a change to send, due to lack of activity.
                                        // Lower the heartbeat time to work around this, and reconnect:
                                        this.heartBeatSeconds = Math.min(this.heartBeatSeconds, (int) (elapsed * 0.75));
                                        Log.v(Log.TAG_CHANGE_TRACKER, "%s: Starting new longpoll", this);
                                        backoff.resetBackoff();
                                        continue;
                                    } else {
                                        Log.d(Log.TAG_CHANGE_TRACKER, "%s: Change tracker calling stop (LongPoll)", this);
                                        client.changeTrackerFinished(this);
                                        break;
                                    }
                                }
                            } else {  // one-shot replications
                                Log.v(Log.TAG_CHANGE_TRACKER, "%s: readValue (oneshot)", this);
                                JsonFactory factory = new JsonFactory();
                                JsonParser jp = factory.createParser(inputStream);
                                JsonToken token;
                                // nextToken() is null => no more token
                                while (((token = jp.nextToken()) != JsonToken.START_ARRAY) &&
                                        (token != null)) {
                                    // ignore these tokens
                                }
                                while (jp.nextToken() == JsonToken.START_OBJECT) {
                                    Map<String, Object> change = (Map) Manager.getObjectMapper().readValue(jp, Map.class);
                                    if (!receivedChange(change)) {
                                        Log.w(Log.TAG_CHANGE_TRACKER, "Received unparseable change line from server: %s", change);
                                    }
                                    // if not running state anymore, exit from loop.
                                    if (!running)
                                        break;
                                }
                                if (jp != null)
                                    jp.close();

                                Log.v(Log.TAG_CHANGE_TRACKER, "%s: /readValue (oneshot)", this);
                                client.changeTrackerCaughtUp();
                                if (isContinuous()) {  // if enclosing replication is continuous
                                    mode = ChangeTrackerMode.LongPoll;
                                } else {
                                    Log.d(Log.TAG_CHANGE_TRACKER, "%s: Change tracker calling stop (OneShot)", this);
                                    client.changeTrackerFinished(this);
                                    break;
                                }
                            }
                            backoff.resetBackoff();
                        } finally {
                            try {
                                if (inputStream != null) {
                                    inputStream.close();
                                    inputStream = null;
                                }
                            } catch (IOException e) {
                            }
                        }
                    }
                } finally {
                    RequestUtils.closeResponseBody(response);
                }
            } catch (Exception e) {
                if (!running && e instanceof IOException) {
                    // in this case, just silently absorb the exception because it
                    // frequently happens when we're shutting down and have to
                    // close the socket underneath our read.
                } else {
                    Log.w(Log.TAG_CHANGE_TRACKER, this + ": Exception in change tracker", e);
                    this.error = e;
                }
                backoff.sleepAppropriateAmountOfTime();
            }
        }
        Log.v(Log.TAG_CHANGE_TRACKER, "%s: Change tracker run loop exiting", this);
    }

    public boolean receivedChange(final Map<String, Object> change) {
        // wait if paused flag is on.
        waitIfPaused();
        // check if still running
        if (running) {
            Object seq = change.get("seq");
            if (seq == null) {
                return false;
            }
            //pass the change to the client on the thread that created this change tracker
            if (client != null) {
                Log.d(Log.TAG_CHANGE_TRACKER, "%s: changeTrackerReceivedChange: %s", this, change);
                client.changeTrackerReceivedChange(change);
                Log.d(Log.TAG_CHANGE_TRACKER, "%s: /changeTrackerReceivedChange: %s", this, change);

            }
            lastSequenceID = seq;
        }
        return true;
    }

    public boolean receivedPollResponse(Map<String, Object> response) {
        List<Map<String, Object>> changes = (List) response.get("results");
        if (changes == null) {
            return false;
        }
        for (Map<String, Object> change : changes) {
            if (!receivedChange(change)) {
                return false;
            }
            // if not running state anymore, exit from loop.
            if (!running)
                break;
        }
        return true;
    }

    public void setUpstreamError(String message) {
        Log.w(Log.TAG_CHANGE_TRACKER, "Server error: %s", message);
        this.error = new Throwable(message);
    }

    public boolean start() {
        Log.d(Log.TAG_CHANGE_TRACKER, "%s: Changed tracker asked to start", this);
        running = true;
        this.error = null;
        String maskedRemoteWithoutCredentials = databaseURL.toExternalForm();
        maskedRemoteWithoutCredentials = maskedRemoteWithoutCredentials.replaceAll("://.*:.*@", "://---:---@");
        thread = new Thread(this, "ChangeTracker-" + maskedRemoteWithoutCredentials);
        thread.start();
        return true;
    }

    public void stop() {
        Log.d(Log.TAG_CHANGE_TRACKER, "%s: Changed tracker asked to stop", this);

        running = false;

        // Awake thread if it is wait for pause
        setPaused(false);

        if (call != null) {
            Log.d(Log.TAG_CHANGE_TRACKER, "%s: Changed tracker aborting request: %s", this, request);
            //request.abort();
            call.cancel();
        }
        if (inputStream != null) {
            try {
                inputStream.close();
                inputStream = null;
            } catch (IOException ex) {
            }
        }

        try {
            if (thread != null) {
                thread.interrupt(); // wake thread if it sleeps, waits, ...
            }
        } catch (Exception e) {
            Log.d(Log.TAG_CHANGE_TRACKER, "%s: Exception interrupting thread: %s", this);
        }
    }

    private void stopped() {
        Log.d(Log.TAG_CHANGE_TRACKER, "%s: Change tracker in stopped()", this);
        running = false; // in case stop() method was not called to stop
        if (client != null) {
            Log.d(Log.TAG_CHANGE_TRACKER, "%s: Change tracker calling changeTrackerStopped, client: %s", this, client);
            client.changeTrackerStopped(ChangeTracker.this);
        } else {
            Log.d(Log.TAG_CHANGE_TRACKER, "%s: Change tracker not calling changeTrackerStopped, client == null", this);
        }
        client = null;
    }

    public void setRequestHeaders(Map<String, Object> requestHeaders) {
        this.requestHeaders = requestHeaders;
    }

    private void addRequestHeaders(Request.Builder builder) {
        if (requestHeaders != null) {
            for (String requestHeaderKey : requestHeaders.keySet()) {
                builder.addHeader(requestHeaderKey, requestHeaders.get(requestHeaderKey).toString());
            }
        }
    }

    public Throwable getLastError() {
        Log.d(Log.TAG_CHANGE_TRACKER, "%s: getLastError() %s", this, error);
        return error;
    }

    public boolean isRunning() {
        return running;
    }

    public void setDocIDs(List<String> docIDs) {
        this.docIDs = docIDs;
    }

    public String changesFeedPOSTBody() {
        Map<String, Object> postBodyMap = changesFeedPOSTBodyMap();
        try {
            return Manager.getObjectMapper().writeValueAsString(postBodyMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    // only for unit test
    /* package */ void setUsePOST(boolean usePOST) {
        this.usePOST = usePOST;
    }

    public Map<String, Object> changesFeedPOSTBodyMap() {
        Object since = lastSequenceID;
        if (lastSequenceID != null && lastSequenceID instanceof String) {
            try {
                Long value = Long.valueOf((String) lastSequenceID);
                if (value.longValue() >= 0)
                    since = value;
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        if (docIDs != null && docIDs.size() > 0) {
            filterName = "_doc_ids";
            filterParams = new HashMap<String, Object>();
            filterParams.put("doc_ids", docIDs);
        }

        Map<String, Object> post = new HashMap<String, Object>();
        post.put("feed", getFeed());
        post.put("heartbeat", getHeartbeatMilliseconds());
        post.put("style", includeConflicts ? "all_docs" : null);
        post.put("active_only", activeOnly && !caughtUp ? true : null);
        post.put("since", since);
        if (filterName != null)
            post.put("filter", filterName);
        post.put("limit", limit > 0 ? limit : null);
        // TODO: {@"accept_encoding", @"gzip"}
        if (filterName != null && filterParams != null)
            post.putAll(filterParams);
        return post;
    }

    public void setPaused(boolean paused) {
        Log.v(Log.TAG, "setPaused: " + paused);
        synchronized (pausedObj) {
            if (this.paused != paused) {
                this.paused = paused;
                pausedObj.notifyAll();
            }
        }
    }

    protected void waitIfPaused() {
        synchronized (pausedObj) {
            while (paused && running) {
                Log.v(Log.TAG, "Waiting: " + paused);
                try {
                    // every 5 sec, wake by myself to check if still needs to pause
                    pausedObj.wait(TIMEOUT_FOR_PAUSE);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    @Override
    public String toString() {
        if (str == null) {
            String remoteURL = databaseURL.toExternalForm().replaceAll("://.*:.*@", "://---:---@");
            str = String.format("ChangeTracker{%s, %s, @%s}", remoteURL, mode, Integer.toHexString(hashCode()));
        }
        return str;
    }
}
