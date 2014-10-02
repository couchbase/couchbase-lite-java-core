package com.couchbase.lite.replicator;

import com.couchbase.lite.Manager;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.auth.AuthenticatorImpl;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.URIUtils;
import com.couchbase.lite.util.Utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Reads the continuous-mode _changes feed of a database, and sends the
 * individual change entries to its client's changeTrackerReceivedChange()
 *
 * @exclude
 */
@InterfaceAudience.Private
public class ChangeTracker implements Runnable {

    private final URL databaseURL;
    private final ChangeTrackerClient client;
    private volatile ChangeTracker.ChangeTrackerMode mode;
    private volatile Object lastSequenceID;
    private final boolean includeConflicts;

    private Thread thread;
    public enum ChangeTrackerStateEnum { NOTSTARTED, NOTRUNNING, RUNNING, STOPPED }
    private AtomicReference<ChangeTrackerStateEnum> trackerState =
            new AtomicReference<ChangeTrackerStateEnum>(ChangeTrackerStateEnum.NOTSTARTED);

    private final String filterName;
    private final Map<String, Object> filterParams;

    private volatile Throwable error = null;
    private final Map<String, Object> requestHeaders;
    private final ChangeTrackerBackoff backoff = new ChangeTrackerBackoff();
    private final boolean usePOST;
    private final static int heartBeatSeconds = 300;
    private final static int limit = 50;
    private final boolean continuous;

    private final Authenticator authenticator;

    public enum ChangeTrackerMode {
        OneShot,
        LongPoll,
        Continuous  // does not work, do not use it.
    }

    /**
     * This constructor is intended for testing purposes.
     * @param databaseURL
     * @param mode
     * @param includeConflicts
     * @param lastSequenceID
     * @param client
     * @param usePOST
     */
    @InterfaceAudience.Private
    public ChangeTracker(URL databaseURL, ChangeTrackerMode mode, boolean includeConflicts,
                         Object lastSequenceID, ChangeTrackerClient client, boolean usePOST) {
        this(databaseURL, mode, includeConflicts, lastSequenceID, client, usePOST, null, null, null, null, false);
    }

    public ChangeTracker(URL databaseURL, ChangeTrackerMode mode, boolean includeConflicts,
                         Object lastSequenceID, ChangeTrackerClient client, boolean usePOST,
                         List<String> docIDs, Map<String, Object> requestHeaders, Authenticator authenticator,
                         boolean continuous) {
        this(databaseURL, mode, includeConflicts, lastSequenceID, client, usePOST, docIDs, null, null,
                requestHeaders, authenticator, continuous);
    }

    public ChangeTracker(URL databaseURL, ChangeTrackerMode mode, boolean includeConflicts,
                         Object lastSequenceID, ChangeTrackerClient client, boolean usePOST,
                         String filterName, Map<String, Object> filterParams,
                         Map<String, Object> requestHeaders, Authenticator authenticator, boolean continuous) {
        this(databaseURL, mode, includeConflicts, lastSequenceID, client, usePOST, null, filterName, filterParams,
                requestHeaders, authenticator, continuous);
    }

    private ChangeTracker(URL databaseURL, ChangeTrackerMode mode, boolean includeConflicts,
                          Object lastSequenceID, ChangeTrackerClient client, boolean usePOST,
                          List<String> docIDs, String filterName, Map<String, Object> filterParams,
                          Map<String, Object> requestHeaders, Authenticator authenticator, boolean continuous) {

        this.continuous = continuous;

        this.mode = mode;
        this.lastSequenceID = lastSequenceID;
        this.includeConflicts = includeConflicts;

        if (databaseURL == null) {
            throw new IllegalArgumentException("databaseURL cannot be NULL");
        }

        this.databaseURL = databaseURL;

        if (client == null) {
            throw new IllegalArgumentException("client cannot be NULL");
        }
        this.client = client;
        this.usePOST = usePOST;

        if (docIDs != null && docIDs.size() > 0 && (filterName != null || filterParams != null)) {
            throw new IllegalArgumentException("Both docIds and filterName/filterParams cannot be simultaneously set.");
        }

        if (docIDs != null && docIDs.size() > 0) {
            filterName = "_doc_ids";
            filterParams = new HashMap<String, Object>();
            filterParams.put("doc_ids", docIDs);
        }

        this.filterName = filterName;
        this.filterParams = filterParams;
        this.requestHeaders = requestHeaders == null ? new HashMap<String, Object>() : requestHeaders;
        this.authenticator = authenticator;
    }

    private boolean isContinuous() {
        return continuous;
    }

    public String getDatabaseName() {
        String result = null;
        if (databaseURL != null) {
            result = databaseURL.getPath();
            if (result != null) {
                int pathLastSlashPos = result.lastIndexOf('/');
                if (pathLastSlashPos > 0) {
                    result = result.substring(pathLastSlashPos);
                }
            }
        }
        return result;
    }

    private String getFeed() {
        switch (mode) {
            case LongPoll:
                return "longpoll";
            case Continuous:
                return "continuous";
            case OneShot:
            default:
                return "normal";
        }
    }

    private long getHeartbeatMilliseconds() {
        return heartBeatSeconds * 1000;
    }

    @InterfaceAudience.Private
    public String getChangesFeedPath() {

        if (usePOST) {
            return "_changes";
        }

        String path = "_changes?feed=";
        path += getFeed();
        if (mode == ChangeTrackerMode.LongPoll) {
            path += String.format("&limit=%s", limit);
        }
        path += String.format("&heartbeat=%s", getHeartbeatMilliseconds());

        if (includeConflicts) {
            path += "&style=all_docs";
        }

        if(lastSequenceID != null) {
            path += "&since=" + URLEncoder.encode(lastSequenceID.toString());
        }

        if(filterName != null) {
            path += "&filter=" + URLEncoder.encode(filterName);
            if(filterParams != null) {
                for (String filterParamKey : filterParams.keySet()) {

                    Object value = filterParams.get(filterParamKey);
                    if (!(value instanceof String)) {
                        try {
                            value = Manager.getObjectMapper().writeValueAsString(value);
                        } catch (IOException e) {
                            throw new IllegalArgumentException(e);
                        }
                    }
                    path += "&" + URLEncoder.encode(filterParamKey) + "=" + URLEncoder.encode(value.toString());

                }
            }
        }

        return path;
    }

    private URL getChangesFeedURL() {
        String dbURLString = databaseURL.toExternalForm();
        if(!dbURLString.endsWith("/")) {
            dbURLString += "/";
        }
        dbURLString += getChangesFeedPath();
        URL result = null;
        try {
            result = new URL(dbURLString);
        } catch(MalformedURLException e) {
            Log.e(Log.TAG_CHANGE_TRACKER, this + ": Changes feed URL is malformed", e);
        }
        return result;
    }

    @Override
    public void run() {
        boolean caughtUp = false;

        if (!trackerState.compareAndSet(ChangeTrackerStateEnum.NOTRUNNING,
                ChangeTrackerStateEnum.RUNNING)) {
            Log.w(Log.TAG_CHANGE_TRACKER,
                    "%s: ChangeTracker run() loop aborting because tracker state isn't NOTRUNNING", this);
            stop();
            return;
        }

        if (mode == ChangeTrackerMode.Continuous) {
            // there is a failing unit test for this, and from looking at the code the Replication
            // object will never use Continuous desiredReplicationMode anyway.  Explicitly prevent its use until
            // it is demonstrated to actually work.
            Log.e(Log.TAG_CHANGE_TRACKER,
                    "ChangeTracker does not correctly support continuous mode");
            stop();
            return;
        }

        HttpClient httpClient = client.getHttpClient();

        HttpUriRequest request = null;
        while (trackerState.get() == ChangeTrackerStateEnum.RUNNING && !Thread.interrupted()) {
            final URL url = getChangesFeedURL();
            if (usePOST) {
                HttpPost postRequest = new HttpPost(url.toString());
                postRequest.setHeader("Content-Type", "application/json");
                StringEntity entity;
                try {
                    entity = new StringEntity(changesFeedPOSTBody());
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
                postRequest.setEntity(entity);
                request = postRequest;

            } else {
                request = new HttpGet(url.toString());
            }

            addRequestHeaders(request);

            // Perform BASIC Authentication if needed
            boolean isUrlBasedUserInfo = false;

            // If the URL contains user info AND if this a DefaultHttpClient then preemptively set the auth credentials
            String userInfo = url.getUserInfo();
            if (userInfo != null) {
                isUrlBasedUserInfo = true;
            } else {
                if (authenticator != null) {
                    AuthenticatorImpl auth = (AuthenticatorImpl) authenticator;
                    userInfo = auth.authUserInfo();
                }
            }

            if (userInfo != null) {
                if (userInfo.contains(":") && !userInfo.trim().equals(":")) {
                    String[] userInfoElements = userInfo.split(":");
                    String username = isUrlBasedUserInfo ? URIUtils.decode(userInfoElements[0]): userInfoElements[0];
                    String password = isUrlBasedUserInfo ? URIUtils.decode(userInfoElements[1]): userInfoElements[1];
                    final Credentials credentials = new UsernamePasswordCredentials(username, password);

                    if (httpClient instanceof DefaultHttpClient) {
                        DefaultHttpClient dhc = (DefaultHttpClient) httpClient;
                        HttpRequestInterceptor preemptiveAuth = new HttpRequestInterceptor() {
                            @Override
                            public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                                AuthState authState = (AuthState) context.getAttribute(ClientContext.TARGET_AUTH_STATE);
                                if (authState.getAuthScheme() == null) {
                                    authState.setAuthScheme(new BasicScheme());
                                    authState.setCredentials(credentials);
                                }
                            }
                        };
                        dhc.addRequestInterceptor(preemptiveAuth, 0);
                    }
                } else {
                    Log.w(Log.TAG_CHANGE_TRACKER, "RemoteRequest Unable to parse user info, not setting credentials");
                }
            }

            try {
                String maskedRemoteWithoutCredentials = getChangesFeedURL().toString();
                maskedRemoteWithoutCredentials = maskedRemoteWithoutCredentials.replaceAll("://.*:.*@", "://---:---@");
                Log.v(Log.TAG_CHANGE_TRACKER, "%s: Making request to %s", this, maskedRemoteWithoutCredentials);
                HttpResponse response = httpClient.execute(request);
                StatusLine status = response.getStatusLine();
                if (status.getStatusCode() >= 300 && !Utils.isTransientError(status)) {
                    Log.e(Log.TAG_CHANGE_TRACKER, "%s: Change tracker got error %d", this, status.getStatusCode());
                    this.error = new HttpResponseException(status.getStatusCode(), status.getReasonPhrase());
                    stop();
                    return;
                }
                HttpEntity entity = response.getEntity();
                Log.v(Log.TAG_CHANGE_TRACKER, "%s: got response. status: %s mode: %s", this, status, mode);
                InputStream input = null;
                if (entity != null) {
                    try {
                        input = entity.getContent();
                        if (mode == ChangeTrackerMode.LongPoll) {  // continuous replications
                            Map<String, Object> fullBody = Manager.getObjectMapper().readValue(input, Map.class);
                            if (receivedPollResponse(fullBody)) {

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
                                Log.w(Log.TAG_CHANGE_TRACKER, "%s: Change tracker calling stop (LongPoll)", this);
                                client.changeTrackerFinished(this);
                                stop();
                            }
                        } else {  // one-shot replications

                            JsonFactory jsonFactory = Manager.getObjectMapper().getJsonFactory();
                            JsonParser jp = jsonFactory.createJsonParser(input);

                            while (jp.nextToken() != JsonToken.START_ARRAY) {
                                // ignore these tokens
                            }

                            while (jp.nextToken() == JsonToken.START_OBJECT) {
                                Map<String, Object> change = (Map) Manager.getObjectMapper().readValue(jp, Map.class);
                                if (!receivedChange(change)) {
                                    Log.w(Log.TAG_CHANGE_TRACKER, "Received unparseable change line from server: %s", change);
                                }

                            }

                            if (!caughtUp) {
                                caughtUp = true;
                                client.changeTrackerCaughtUp();
                            }

                            if (isContinuous()) { // If enclosing replication is continuous
                                mode = ChangeTrackerMode.LongPoll;
                            } else {
                                Log.w(Log.TAG_CHANGE_TRACKER, "%s: Change tracker calling stop (OneShot)", this);
                                client.changeTrackerFinished(this);
                                stop();
                                break;  // break out of while (running) loop
                            }

                        }

                        backoff.resetBackoff();
                    } finally {
                        try {
                            entity.consumeContent();
                        } catch (IOException ex) {
                        }
                    }

                }
            } catch (Exception e) {

                if (trackerState.get() != ChangeTrackerStateEnum.RUNNING && e instanceof IOException) {
                    // in this case, just silently absorb the exception because it
                    // frequently happens when we're shutting down and have to
                    // close the socket underneath our read.
                } else {
                    Log.e(Log.TAG_CHANGE_TRACKER, this + ": Exception in change tracker", e);
                }

                backoff.sleepAppropriateAmountOfTime();

            }
        }

        if (request != null) {
            Log.d(Log.TAG_CHANGE_TRACKER, "%s: Changed tracker aborting request: %s", this, request);
            request.abort();
        }

        Log.v(Log.TAG_CHANGE_TRACKER, "%s: Change tracker run loop exiting", this);
    }

    private boolean receivedChange(final Map<String,Object> change) {
        Object seq = change.get("seq");
        if(seq == null) {
            return false;
        }
        //pass the change to the client on the thread that created this change tracker
        if(trackerState.get() == ChangeTrackerStateEnum.RUNNING) {
            client.changeTrackerReceivedChange(change);
        }
        lastSequenceID = seq;
        return true;
    }

    private boolean receivedPollResponse(Map<String,Object> response) {
        List<Map<String,Object>> changes = (List)response.get("results");
        if(changes == null) {
            return false;
        }
        for (Map<String,Object> change : changes) {
            if(!receivedChange(change)) {
                return false;
            }
        }
        return true;
    }

    public void setUpstreamError(String message) {
        Log.w(Log.TAG_CHANGE_TRACKER, "Server error: %s", message);
        this.error = new Throwable(message);
    }

    /**
     * Starts the change tracker. Can only be called once or it will throw an exception.
     * @return
     */
    public boolean start() {
        if (!trackerState.compareAndSet(ChangeTrackerStateEnum.NOTSTARTED, ChangeTrackerStateEnum.NOTRUNNING)) {
            Log.e(Log.TAG_CHANGE_TRACKER, this + ": start called twice or after stop");
            throw new RuntimeException("Start got called twice or after stop.");
        }

        Log.d(Log.TAG_CHANGE_TRACKER, "%s: Changed tracker asked to start", this);
        String maskedRemoteWithoutCredentials = databaseURL.toExternalForm();
        maskedRemoteWithoutCredentials = maskedRemoteWithoutCredentials.replaceAll("://.*:.*@", "://---:---@");
        thread = new Thread(this, "ChangeTracker-" + maskedRemoteWithoutCredentials);
        thread.start();
        return true;
    }

    /**
     * Because stop can be called from exception handlers, finally clauses etc., we allow it to be called
     * multiple times without harm.
     */
    public void stop() {
        ChangeTrackerStateEnum currentEnum = trackerState.getAndSet(ChangeTrackerStateEnum.STOPPED);
        Log.d(Log.TAG_CHANGE_TRACKER, "%s: Changed tracker asked to stop", this);
        if (currentEnum != ChangeTrackerStateEnum.STOPPED) {
            Thread localThread = thread;
            if (localThread != null) {
                localThread.interrupt();
            }
            Log.w(Log.TAG_CHANGE_TRACKER, "%s: Change tracker calling changeTrackerStopped, client: %s", this, client);
            client.changeTrackerStopped(ChangeTracker.this);
        }
    }

    private void addRequestHeaders(HttpUriRequest request) {
        if (requestHeaders != null) {
            for (String requestHeaderKey : requestHeaders.keySet()) {
                request.addHeader(requestHeaderKey, requestHeaders.get(requestHeaderKey).toString());
            }
        }
    }

    public Throwable getLastError() {
        return error;
    }

    @InterfaceAudience.Private
    public String changesFeedPOSTBody() {
        Map<String, Object> postBodyMap = changesFeedPOSTBodyMap();
        try {
            return Manager.getObjectMapper().writeValueAsString(postBodyMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @InterfaceAudience.Private
    public Map<String, Object> changesFeedPOSTBodyMap() {

        if (!usePOST) {
            return null;
        }

        Map<String, Object> post = new HashMap<String, Object>();
        post.put("feed", getFeed());
        post.put("heartbeat", getHeartbeatMilliseconds());
        if (includeConflicts) {
            post.put("style","all_docs");
        } else {
            post.put("style", null);
        }
        if (lastSequenceID != null) {
            try {
                post.put("since", Long.parseLong(lastSequenceID.toString()));
            } catch (NumberFormatException e) {
                post.put("since", lastSequenceID.toString());
            }
        }

        if (mode == ChangeTrackerMode.LongPoll && limit > 0) {
            post.put("limit", limit);
        } else {
            post.put("limit", null);
        }

        if (filterName != null) {
            post.put("filter", filterName);
            if (filterParams != null) {
                post.putAll(filterParams);
            }
        }

        return post;
    }

    @InterfaceAudience.Private
    public ChangeTrackerBackoff getBackoff() {
        return backoff;
    }

}
