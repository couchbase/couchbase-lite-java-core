//
// Copyright (c) 2016 Couchbase, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
//
package com.couchbase.lite.replicator;

import com.couchbase.lite.Manager;
import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.CancellableRunnable;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import okhttp3.Call;
import okhttp3.Cookie;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * @exclude
 */
public class RemoteRequest implements CancellableRunnable {
    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////

    public static final String TAG = Log.TAG_REMOTE_REQUEST;
    // Don't compress data shorter than this (not worth the CPU time, plus it might not shrink)
    public static final int MIN_JSON_LENGTH_TO_COMPRESS = 100;
    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    protected final HttpClientFactory factory;
    protected String method;
    protected URL url;
    protected Map<String, ?> body;
    protected Authenticator authenticator;
    protected RemoteRequestCompletion onPreCompletion;
    protected RemoteRequestCompletion onCompletion;
    protected RemoteRequestCompletion onPostCompletion;
    protected Call call;
    private boolean cancelable = true;
    protected Map<String, Object> requestHeaders;
    // if true, we wont log any 404 errors
    protected boolean dontLog404;
    // if true, send compressed (gzip) request
    protected boolean compressedRequest = false;
    protected String str = null;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////
    public RemoteRequest(
            HttpClientFactory factory,
            String method,
            URL url,
            boolean cancelable,
            Map<String, ?> body,
            Map<String, Object> requestHeaders,
            RemoteRequestCompletion onCompletion) {
        this.factory = factory;
        this.method = method;
        this.url = url;
        this.cancelable = cancelable;
        this.body = body;
        this.onCompletion = onCompletion;
        this.requestHeaders = requestHeaders;
        Log.v(Log.TAG_SYNC, "%s: RemoteRequest created, url: %s", this, url);
    }

    ////////////////////////////////////////////////////////////
    // Implementations of CancellableRunnable
    ////////////////////////////////////////////////////////////

    @Override
    public void run() {
        execute();
    }

    @Override
    public void cancel() {
        if (call != null && !call.isCanceled() && cancelable) {
            Log.w(TAG, "%s: aborting request: %s", this, call);
            call.cancel();
        }
    }

    ////////////////////////////////////////////////////////////
    // Public Methods
    ////////////////////////////////////////////////////////////
    @Override
    public String toString() {
        if (str == null) {
            String remoteURL = url.toExternalForm().replaceAll("://.*:.*@", "://---:---@");
            str = String.format(Locale.ENGLISH, "%s {%s, %s}", this.getClass().getName(), method, remoteURL);
        }
        return str;
    }

    ////////////////////////////////////////////////////////////
    // Setter / Getter methods
    ////////////////////////////////////////////////////////////

    public void setOnPostCompletion(RemoteRequestCompletion onPostCompletion) {
        this.onPostCompletion = onPostCompletion;
    }

    public void setOnPreCompletion(RemoteRequestCompletion onPreCompletion) {
        this.onPreCompletion = onPreCompletion;
    }

    public void setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    public void setDontLog404(boolean dontLog404) {
        this.dontLog404 = dontLog404;
    }

    public boolean isCompressedRequest() {
        return compressedRequest;
    }

    public void setCompressedRequest(boolean compressedRequest) {
        this.compressedRequest = compressedRequest;
    }

    ////////////////////////////////////////////////////////////
    // Protected or Private Methods
    ////////////////////////////////////////////////////////////

    /**
     * Execute remote request
     */
    protected void execute() {
        Log.v(Log.TAG_SYNC, "%s: RemoteRequest execute() called, url: %s", this, url);
        executeRequest(factory.getOkHttpClient(), request());
        Log.v(Log.TAG_SYNC, "%s: RemoteRequest execute() finished, url: %s", this, url);
    }

    /**
     * Prepare request
     */
    protected Request request() {
        Request.Builder builder = new Request.Builder().url(url);
        builder = RequestUtils.preemptivelySetAuthCredentials(builder, url, authenticator);
        addHeaders(builder);
        setBody(builder);
        return builder.build();
    }

    /**
     * set headers
     */
    protected Request.Builder addHeaders(Request.Builder builder) {
        builder.addHeader("Accept", "multipart/related, application/json");
        builder.addHeader("User-Agent", Manager.getUserAgent());
        builder.addHeader("Accept-Encoding", "gzip, deflate");
        addRequestHeaders(builder);
        return builder;
    }

    protected Request.Builder addRequestHeaders(Request.Builder builder) {
        if (requestHeaders != null) {
            for (String key : requestHeaders.keySet()) {
                builder.addHeader(key, requestHeaders.get(key).toString());
            }
        }
        return builder;
    }

    /**
     * set request body
     */
    protected Request.Builder setBody(Request.Builder builder) {
        if (body != null) {
            byte[] bodyBytes = null;
            try {
                bodyBytes = Manager.getObjectMapper().writeValueAsBytes(body);
            } catch (Exception e) {
                Log.e(TAG, "Error serializing body of request", e);
            }
            RequestBody requestBody = null;
            if (isCompressedRequest()) {
                requestBody = setCompressedBody(bodyBytes);
                if (requestBody != null)
                    builder.addHeader("Content-Encoding", "gzip");
            }
            if (requestBody == null)
                requestBody = RequestBody.create(JSON, bodyBytes);
            if ("PUT".equalsIgnoreCase(method))
                builder.put(requestBody);
            else if ("POST".equalsIgnoreCase(method))
                builder.post(requestBody);
        }
        return builder;
    }

    /**
     * Generate gzipped body
     */
    protected RequestBody setCompressedBody(byte[] bodyBytes) {
        if (bodyBytes.length < MIN_JSON_LENGTH_TO_COMPRESS)
            return null;
        byte[] encodedBytes = Utils.compressByGzip(bodyBytes);
        if (encodedBytes == null || encodedBytes.length >= bodyBytes.length)
            return null;
        return RequestBody.create(JSON, encodedBytes);
    }

    /**
     * Execute request
     */
    protected void executeRequest(OkHttpClient httpClient, Request request) {
        Object fullBody = null;
        Throwable error = null;
        Response response = null;
        try {
            try {
                Log.v(TAG, "%s: RemoteRequest calling httpClient.execute, url: %s", this, url);
                call = httpClient.newCall(request);
                response = call.execute();
                Log.v(TAG, "%s: RemoteRequest called httpClient.execute, url: %s", this, url);
                storeCookie(response);
                // error
                if (response.code() >= 300) {
                    if (!dontLog404)
                        Log.w(TAG, "%s: Got error status: %d for %s. Reason: %s",
                                this, response.code(), url, response.message());
                    String authHeader = response.header("WWW-Authenticate");
                    Map<String, String> challenge = parseAuthHeader(authHeader);
                    Map<String, Object> extra = null;
                    if (challenge != null) {
                        extra = new HashMap();
                        extra.put("AuthChallenge", challenge);
                    }
                    error = new RemoteRequestResponseException(response.code(), response.message(), extra);
                    RequestUtils.closeResponseBody(response);
                }
                // success
                else {
                    InputStream stream = response.body().byteStream();
                    try {
                        // decompress if contentEncoding is gzip
                        if (Utils.isGzip(response))
                            stream = new GZIPInputStream(stream);
                        fullBody = Manager.getObjectMapper().readValue(stream, Object.class);
                    } finally {
                        try {
                            stream.close();
                        } catch (IOException e) {
                        }
                    }
                }
            } catch (Exception e) {
                // call.execute(), GZIPInputStream, or ObjectMapper.readValue()
                Log.w(TAG, "%s: executeRequest() Exception: %s.  url: %s", e, this, e, url);
                error = e;
            }
            respondWithResult(fullBody, error, response);
        } finally {
            RequestUtils.closeResponseBody(response);
        }
    }

    protected void respondWithResult(final Object result,
                                     final Throwable error,
                                     final Response response) {
        try {
            if (onPreCompletion != null)
                onPreCompletion.onCompletion(response, null, error);
            onCompletion.onCompletion(response, result, error);
            if (onPostCompletion != null)
                onPostCompletion.onCompletion(response, null, error);
        } catch (Exception e) {
            Log.e(TAG, "RemoteRequestCompletionBlock throw Exception", e);
        }
    }

    protected void storeCookie(Response response) {
        if (!response.headers("Set-Cookie").isEmpty()) {
            HttpUrl rUrl = response.request().url();
            HttpUrl url = new HttpUrl.Builder()
                    .scheme(rUrl.scheme())
                    .host(rUrl.host())
                    .build();
            List<Cookie> cookies = new ArrayList<Cookie>();
            for (String setCookie : response.headers("Set-Cookie")) {
                cookies.add(Cookie.parse(url, setCookie));
            }
            factory.addCookies(cookies);
        }
    }

    static Pattern re = Pattern.compile("(\\w+)\\s+(\\w+)=((\\w+)|\"([^\"]+))");

    /**
     * BLIPHTTPLogic.m
     * + (NSDictionary*) parseAuthHeader: (NSString*)authHeader
     *
     * @param authHeader
     * @return
     */
    protected static Map<String, String> parseAuthHeader(String authHeader) {
        if (authHeader == null || authHeader.length() == 0)
            return null;
        Map<String, String> challenge = new HashMap<String, String>();
        Matcher m = re.matcher(authHeader);
        while (m.find()) {
            String scheme = m.group(1);
            String key = m.group(2);
            String value = m.group(4);
            if (value == null || value.length() == 0)
                value = m.group(5);
            challenge.put(key, value);
            challenge.put("Scheme", scheme);
        }
        challenge.put("WWW-Authenticate", authHeader);
        return challenge;
    }
}
