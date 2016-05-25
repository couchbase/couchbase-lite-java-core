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
import com.couchbase.lite.Manager;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.MultipartDocumentReader;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class RemoteMultipartDownloaderRequest extends RemoteRequest {
    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    private static final int BUF_LEN = 1024;

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    private Database db;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    public RemoteMultipartDownloaderRequest(
            HttpClientFactory clientFactory,
            String method,
            URL url,
            boolean cancelable,
            Map<String, ?> body,
            Database db,
            Map<String, Object> requestHeaders,
            RemoteRequestCompletion onCompletion) {
        super(clientFactory, method, url, cancelable, body, requestHeaders, onCompletion);
        this.db = db;
    }

    ////////////////////////////////////////////////////////////
    // Override methods
    ////////////////////////////////////////////////////////////

    /**
     * set headers
     */
    protected Request.Builder addHeaders(Request.Builder builder) {
        builder.addHeader("Accept", "multipart/related, application/json");
        builder.addHeader("User-Agent", Manager.getUserAgent());
        builder.addHeader("Accept-Encoding", "gzip, deflate");
        builder.addHeader("X-Accept-Part-Encoding", "gzip"); // <-- removable??
        return addRequestHeaders(builder);
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
                Log.v(TAG, "%s: RemoteMultipartDownloaderRequest call execute(), url: %s", this, url);
                call = httpClient.newCall(request);
                response = call.execute();
                Log.v(TAG, "%s: RemoteMultipartDownloaderRequest called execute(), url: %s", this, url);
                storeCookie(response);
                // error
                if (response.code() >= 300) {
                    Log.w(TAG, "%s: Got error status: %d for %s. Reason: %s",
                            this, response.code(), url, response.message());
                    error = new RemoteRequestResponseException(response.code(), response.message());
                    RequestUtils.closeResponseBody(response);
                }
                // success
                else {
                    ResponseBody responseBody = response.body();
                    InputStream stream = responseBody.byteStream();
                    try {
                        // decompress if contentEncoding is gzip
                        if (Utils.isGzip(response))
                            stream = new GZIPInputStream(stream);
                        MediaType type = responseBody.contentType();
                        if (type != null) {
                            // multipart
                            if (type.type().equals("multipart") &&
                                    type.subtype().equals("related")) {
                                MultipartDocumentReader reader = new MultipartDocumentReader(db);
                                reader.setHeaders(Utils.headersToMap(response.headers()));
                                byte[] buffer = new byte[BUF_LEN];
                                int numBytesRead;
                                while ((numBytesRead = stream.read(buffer)) != -1)
                                    reader.appendData(buffer, 0, numBytesRead);
                                reader.finish();
                                fullBody = reader.getDocumentProperties();
                            }
                            // JSON (non-multipart)
                            else
                                fullBody = Manager.getObjectMapper().readValue(
                                        stream, Object.class);
                        }
                    } finally {
                        try {
                            stream.close();
                        } catch (IOException e) {
                        }
                    }
                }
            } catch (Exception e) {
                // call.execute(), GZIPInputStream, or ObjectMapper.readValue()
                Log.w(TAG, "%s: executeRequest() Exception: %s.  url: %s", this, e, url);
                error = e;
            }
            respondWithResult(fullBody, error, response);
        } finally {
            RequestUtils.closeResponseBody(response);
        }
    }
}
