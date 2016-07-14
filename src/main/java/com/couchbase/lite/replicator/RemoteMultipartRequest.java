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

import com.couchbase.lite.BlobKey;
import com.couchbase.lite.BlobStore;
import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.util.Log;

import java.net.URL;
import java.util.Locale;
import java.util.Map;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;

public class RemoteMultipartRequest extends RemoteRequest {
    /////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////
    private Map<String, Object> attachments;
    private Database db;
    private boolean syncGateway;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    public RemoteMultipartRequest(
            HttpClientFactory clientFactory,
            String method,
            URL url,
            boolean syncGateway,
            boolean cancelable,
            Map<String, ?> body,
            Map<String, Object> attachments,
            Database db,
            Map<String, Object> requestHeaders,
            RemoteRequestCompletion onCompletion) {
        super(clientFactory, method, url, cancelable, body, requestHeaders, onCompletion);
        this.attachments = attachments;
        this.db = db;
        this.syncGateway = syncGateway;
    }

    ////////////////////////////////////////////////////////////
    // Override methods
    ////////////////////////////////////////////////////////////

    /**
     * set headers
     */
    @Override
    protected Request.Builder addHeaders(Request.Builder builder) {
        builder.addHeader("Accept", "*/*");
        builder.addHeader("User-Agent", Manager.getUserAgent());
        builder.addHeader("Accept-Encoding", "gzip, deflate");
        return addRequestHeaders(builder);
    }

    /**
     * set request body
     */
    @Override
    protected Request.Builder setBody(Request.Builder builder) {
        if (body != null) {
            MultipartBody.Builder bodyBuilder = createMultipartBody();
            RequestBody requestBody = bodyBuilder.build();
            if ("PUT".equalsIgnoreCase(method))
                builder.put(requestBody);
            else if ("POST".equalsIgnoreCase(method))
                builder.post(requestBody);
        }
        return builder;
    }

    private MultipartBody.Builder createMultipartBody() {
        MultipartBody.Builder multipartBodyBuilder = new MultipartBody.Builder();

        // https://en.wikipedia.org/wiki/MIME#Related
        multipartBodyBuilder.setType(MediaType.parse("multipart/related"));

        // JSON body
        byte[] bodyBytes = null;
        try {
            bodyBytes = Manager.getObjectMapper().writeValueAsBytes(body);
        } catch (Exception e) {
            Log.e(TAG, "Error serializing body of request", e);
        }
        if (bodyBytes != null) {
            RequestBody requestBody = null;
            // gzip??
            if (isCompressedRequest()) {
                requestBody = setCompressedBody(bodyBytes);
                if (requestBody != null)
                    multipartBodyBuilder.addPart(
                            Headers.of("Content-Encoding", "gzip"), requestBody);
            }
            // not gzipped
            if (requestBody == null) {
                requestBody = RequestBody.create(JSON, bodyBytes);
                if (requestBody != null)
                    multipartBodyBuilder.addPart(requestBody);
            }
        }

        // Attachments
        for (String key : attachments.keySet()) {
            Map<String, Object> attachment = (Map<String, Object>) attachments.get(key);
            if (attachment.containsKey("follows")) {
                BlobStore blobStore = db.getAttachmentStore();
                String base64Digest = (String) attachment.get("digest");
                BlobKey blobKey = new BlobKey(base64Digest);

                String contentType = null;
                if (attachment.containsKey("content_type")) {
                    contentType = (String) attachment.get("content_type");
                } else if (attachment.containsKey("type")) {
                    contentType = (String) attachment.get("type");
                } else if (attachment.containsKey("content-type")) {
                    Log.w(Log.TAG_SYNC, "Found attachment that uses content-type" +
                            " field name instead of content_type (see couchbase-lite-android" +
                            " issue #80): %s", attachment);
                }
                // contentType = null causes Exception from FileBody of apache.
                if (contentType == null)
                    contentType = "application/octet-stream"; // default
                // https://github.com/couchbase/couchbase-lite-ios/blob/feb7ff5eda1e80bd00e5eb19f1d46c793f7a1951/Source/CBL_Pusher.m#L449-L452
                String contentEncoding = null;
                if (attachment.containsKey("encoding"))
                    contentEncoding = (String) attachment.get("encoding");
                Headers.Builder builder = new Headers.Builder();
                builder.add("Content-Disposition", String.format(Locale.ENGLISH, "attachment; filename=%s", key));
                //builder.add("Content-Type", contentType);
                if (contentEncoding != null)
                    builder.add("Content-Encoding", contentEncoding);

                // check content-length which is stored in attachments table.
                long declaredLength = 0;
                if(attachment.containsKey("length"))
                    declaredLength = ((Integer)attachment.get("length")).longValue();

                MediaType type = MediaType.parse(contentType);
                RequestBody body = BlobRequestBody.create(type, blobStore, blobKey, declaredLength, syncGateway);
                multipartBodyBuilder.addPart(builder.build(), body);
            }
        }

        return multipartBodyBuilder;
    }
}
