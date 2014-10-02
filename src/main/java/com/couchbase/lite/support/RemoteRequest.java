package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.auth.AuthenticatorImpl;
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
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @exclude
 */
public class RemoteRequest implements Runnable {

    protected ScheduledExecutorService workExecutor;
    protected final HttpClientFactory clientFactory;
    protected String method;
    protected URL url;
    protected Object body;
    protected Authenticator authenticator;
    protected RemoteRequestCompletionBlock onPreCompletion;
    protected RemoteRequestCompletionBlock onCompletion;
    protected RemoteRequestCompletionBlock onPostCompletion;
    private int retryCount;
    private Database db;
    protected HttpUriRequest request;

    protected Map<String, Object> requestHeaders;

    public RemoteRequest(ScheduledExecutorService workExecutor,
                         HttpClientFactory clientFactory, String method, URL url,
                         Object body, Database db, Map<String, Object> requestHeaders, RemoteRequestCompletionBlock onCompletion) {
        this.clientFactory = clientFactory;
        this.method = method;
        this.url = url;
        this.body = body;
        this.onCompletion = onCompletion;
        this.workExecutor = workExecutor;
        this.requestHeaders = requestHeaders;
        this.db = db;
        this.request = createConcreteRequest();
        Log.v(Log.TAG_SYNC, "%s: RemoteRequest created, url: %s", this, url);

    }

    @Override
    public void run() {

        try {
            Log.v(Log.TAG_SYNC, "%s: RemoteRequest run() called, url: %s", this, url);

            HttpClient httpClient = clientFactory.getHttpClient();

            ClientConnectionManager manager = httpClient.getConnectionManager();

            preemptivelySetAuthCredentials(httpClient);

            request.addHeader("Accept", "multipart/related, application/json");

            addRequestHeaders(request);

            setBody(request);

            executeRequest(httpClient, request);

            Log.v(Log.TAG_SYNC, "%s: RemoteRequest run() finished, url: %s", this, url);

        } catch (Throwable e) {
            Log.e(Log.TAG_SYNC, "RemoteRequest.run() exception: %s", e);
        }


    }

    public void abort() {
        Log.w(Log.TAG_REMOTE_REQUEST, "%s: aborting request: %s", this, request);
        if (request != null) {
            request.abort();
        } else {
            Log.w(Log.TAG_REMOTE_REQUEST, "%s: Unable to abort request since underlying request is null", this);
        }
    }

    public HttpUriRequest getRequest() {
        return request;
    }

    protected void addRequestHeaders(HttpUriRequest request) {
        if (requestHeaders != null) {
            for (String requestHeaderKey : requestHeaders.keySet()) {
                request.addHeader(requestHeaderKey, requestHeaders.get(requestHeaderKey).toString());
            }
        }
    }

    public void setOnPostCompletion(RemoteRequestCompletionBlock onPostCompletion) {
        this.onPostCompletion = onPostCompletion;
    }

    public void setOnPreCompletion(RemoteRequestCompletionBlock onPreCompletion) {
        this.onPreCompletion = onPreCompletion;
    }

    protected HttpUriRequest createConcreteRequest() {
        HttpUriRequest request = null;
        if (method.equalsIgnoreCase("GET")) {
            request = new HttpGet(url.toExternalForm());
        } else if (method.equalsIgnoreCase("PUT")) {
            request = new HttpPut(url.toExternalForm());
        } else if (method.equalsIgnoreCase("POST")) {
            request = new HttpPost(url.toExternalForm());
        }
        return request;
    }

    protected void setBody(HttpUriRequest request) {
        // set body if appropriate
        if (body != null && request instanceof HttpEntityEnclosingRequestBase) {
            byte[] bodyBytes = null;
            try {
                bodyBytes = Manager.getObjectMapper().writeValueAsBytes(body);
            } catch (Exception e) {
                Log.e(Log.TAG_REMOTE_REQUEST, "Error serializing body of request", e);
            }
            ByteArrayEntity entity = new ByteArrayEntity(bodyBytes);
            entity.setContentType("application/json");
            ((HttpEntityEnclosingRequestBase) request).setEntity(entity);
        }
    }

    /**
     *  Set Authenticator for BASIC Authentication
     */
    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    protected void executeRequest(HttpClient httpClient, HttpUriRequest requestParam) {

        Object fullBody = null;
        Throwable error = null;
        HttpResponse response = null;
        retryCount = 0;

        try {

            fullBody = null;
            error = null;
            response = null;

            Log.v(Log.TAG_SYNC, "%s: RemoteRequest calling httpClient.execute, url: %s", this, url);

            if (requestParam.isAborted()) {
                Log.v(Log.TAG_SYNC, "%s: RemoteRequest has already been aborted", this);
                respondWithResult(fullBody, new Exception(String.format("%s: Request %s has been aborted", this, requestParam)), response);
                return;
            }

            Log.v(Log.TAG_SYNC, "%s: RemoteRequest calling httpClient.execute, client: %s url: %s", this, httpClient, url);

            response = httpClient.execute(requestParam);

            Log.v(Log.TAG_SYNC, "%s: RemoteRequest called httpClient.execute, url: %s", this, url);

            // add in cookies to global store
            try {
                if (httpClient instanceof DefaultHttpClient) {
                    DefaultHttpClient defaultHttpClient = (DefaultHttpClient)httpClient;
                    this.clientFactory.addCookies(defaultHttpClient.getCookieStore().getCookies());
                }
            } catch (Exception e) {
                Log.e(Log.TAG_REMOTE_REQUEST, "Unable to add in cookies to global store", e);
            }

            StatusLine status = response.getStatusLine();


            if (status.getStatusCode() >= 300) {
                Log.e(Log.TAG_REMOTE_REQUEST, "Got error status: %d for %s.  Reason: %s", status.getStatusCode(), url, status.getReasonPhrase());
                error = new HttpResponseException(status.getStatusCode(),
                        status.getReasonPhrase());
                respondWithResult(fullBody, error, response);
                return;
            } else {
                HttpEntity temp = response.getEntity();
                if (temp != null) {
                    InputStream stream = null;
                    try {
                        stream = temp.getContent();
                        fullBody = Manager.getObjectMapper().readValue(stream,
                                Object.class);
                    } finally {
                        try {
                            stream.close();
                        } catch (IOException e) {
                        }
                    }
                }
            }
        } catch (IOException e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "io exception.  url: %s", e, url);
            error = e;
            // Treat all IOExceptions as transient, per:
            // http://hc.apache.org/httpclient-3.x/exception-handling.html
        } catch (IllegalStateException e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "%s: executeRequest() Exception: %s.  url: %s", this, e, url);
            error = e;
        } catch (Exception e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "%s: executeRequest() Exception: %s.  url: %s", this, e, url);
            error = e;
        }
        finally {
            Log.v(Log.TAG_SYNC, "%s: RemoteRequest finally block.  url: %s", this, url);
        }


        Log.v(Log.TAG_SYNC, "%s: RemoteRequest calling respondWithResult.  url: %s, error: %s", this, url, error);
        respondWithResult(fullBody, error, response);

    }

    protected void preemptivelySetAuthCredentials(HttpClient httpClient) {
        boolean isUrlBasedUserInfo = false;

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
                Log.w(Log.TAG_REMOTE_REQUEST, "RemoteRequest Unable to parse user info, not setting credentials");
            }
        }
    }

    public void respondWithResult(final Object result, final Throwable error, final HttpResponse response) {

        try {
            if (onPreCompletion != null) {
                onPreCompletion.onCompletion(response, null, error);
            }
            onCompletion.onCompletion(response, result, error);
            if (onPostCompletion != null) {
                onPostCompletion.onCompletion(response, null, error);
            }
        } catch (Exception e) {
            // don't let this crash the thread
            Log.e(Log.TAG_REMOTE_REQUEST,
                    "RemoteRequestCompletionBlock throw Exception",
                    e);
        }

    }

}
