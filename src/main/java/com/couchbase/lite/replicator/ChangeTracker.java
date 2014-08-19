package com.couchbase.lite.replicator;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Status;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.auth.AuthenticatorImpl;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.URIUtils;
import com.couchbase.lite.util.Utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.ExecutionContext;
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

import javax.management.relation.RoleUnresolved;


/**
 * Reads the continuous-mode _changes feed of a database, and sends the
 * individual change entries to its client's changeTrackerReceivedChange()
 *
 * @exclude
 */
@InterfaceAudience.Private
public class ChangeTracker implements Runnable {

    private URL databaseURL;
    private ChangeTrackerClient client;
    private ChangeTrackerMode mode;
    private Object lastSequenceID;
    private boolean includeConflicts;

    private Thread thread;
    private boolean running = false;
    private HttpUriRequest request;

    private String filterName;
    private Map<String, Object> filterParams;
    private List<String> docIDs;

    private Throwable error;
    protected Map<String, Object> requestHeaders;
    protected ChangeTrackerBackoff backoff;
    private boolean usePOST;
    private int heartBeatSeconds;
    private int limit;
    private boolean caughtUp = false;
    private boolean continuous = false;  // is enclosing replication continuous?

    private Authenticator authenticator;

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
        this.heartBeatSeconds = 300;
        this.limit = 50;
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

        if (docIDs != null && docIDs.size() > 0) {
            filterName = "_doc_ids";
            filterParams = new HashMap<String, Object>();
            filterParams.put("doc_ids", docIDs);
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

    public URL getChangesFeedURL() {
        String dbURLString = databaseURL.toExternalForm();
        if(!dbURLString.endsWith("/")) {
            dbURLString += "/";
        }
        dbURLString += getChangesFeedPath();
        URL result = null;
        try {
            result = new URL(dbURLString);
        } catch(MalformedURLException e) {
            Log.e(Log.TAG_CHANGE_TRACKER, this + ": Changes feed ULR is malformed", e);
        }
        return result;
    }

    /**
     *  Set Authenticator for BASIC Authentication
     */
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public void run() {

        running = true;
        HttpClient httpClient;

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

        httpClient = client.getHttpClient();
        backoff = new ChangeTrackerBackoff();

        while (running) {

            URL url = getChangesFeedURL();
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
                            boolean responseOK = receivedPollResponse(fullBody);
                            if (mode == ChangeTrackerMode.LongPoll && responseOK) {

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

                            if (isContinuous()) {  // if enclosing replication is continuous
                                mode = ChangeTrackerMode.LongPoll;
                            } else {
                                Log.w(Log.TAG_CHANGE_TRACKER, "%s: Change tracker calling stop (OneShot)", this);
                                client.changeTrackerFinished(this);
                                stopped();
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

                if (!running && e instanceof IOException) {
                    // in this case, just silently absorb the exception because it
                    // frequently happens when we're shutting down and have to
                    // close the socket underneath our read.
                } else {
                    Log.e(Log.TAG_CHANGE_TRACKER, this + ": Exception in change tracker", e);
                }

                backoff.sleepAppropriateAmountOfTime();

            }
        }
        Log.v(Log.TAG_CHANGE_TRACKER, "%s: Change tracker run loop exiting", this);
    }

    public boolean receivedChange(final Map<String,Object> change) {
        Object seq = change.get("seq");
        if(seq == null) {
            return false;
        }
        //pass the change to the client on the thread that created this change tracker
        if(client != null) {
            client.changeTrackerReceivedChange(change);
        }
        lastSequenceID = seq;
        return true;
    }

    public boolean receivedPollResponse(Map<String,Object> response) {
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

    public boolean start() {
        Log.d(Log.TAG_CHANGE_TRACKER, "%s: Changed tracker asked to start", this);
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
        thread.interrupt();
        if(request != null) {
            Log.d(Log.TAG_CHANGE_TRACKER, "%s: Changed tracker aborting request: %s", this, request);
            request.abort();
        }

        stopped();
    }

    public void stopped() {
        Log.d(Log.TAG_CHANGE_TRACKER, "%s: Change tracker in stopped()", this);
        if (client != null) {
            Log.w(Log.TAG_CHANGE_TRACKER, "%s: Change tracker calling changeTrackerStopped, client: %s", this, client);
            client.changeTrackerStopped(ChangeTracker.this);
        } else {
            Log.w(Log.TAG_CHANGE_TRACKER, "%s: Change tracker not calling changeTrackerStopped, client == null", this);
        }
        client = null;
    }

    void setRequestHeaders(Map<String, Object> requestHeaders) {
        this.requestHeaders = requestHeaders;
    }

    private void addRequestHeaders(HttpUriRequest request) {
        for (String requestHeaderKey : requestHeaders.keySet()) {
            request.addHeader(requestHeaderKey, requestHeaders.get(requestHeaderKey).toString());
        }
    }

    public Throwable getLastError() {
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

    public boolean isUsePOST() {
        return usePOST;
    }

    public void setUsePOST(boolean usePOST) {
        this.usePOST = usePOST;
    }

    public Map<String, Object> changesFeedPOSTBodyMap() {

        if (!usePOST) {
            return null;
        }

        if (docIDs != null && docIDs.size() > 0) {
            filterName = "_doc_ids";
            filterParams = new HashMap<String, Object>();
            filterParams.put("doc_ids", docIDs);
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
            post.putAll(filterParams);
        }

        return post;

    }

}
