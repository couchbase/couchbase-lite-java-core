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
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.MultipartDocumentReader;
import com.couchbase.lite.support.MultipartReader;
import com.couchbase.lite.support.MultipartReaderDelegate;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * A special type of RemoteRequest that knows how to call the _bulk_get endpoint.
 */
@InterfaceAudience.Private
public class RemoteBulkDownloaderRequest extends RemoteRequest implements MultipartReaderDelegate {

    @InterfaceAudience.Private
    public interface BulkDownloaderDocument {
        void onDocument(Map<String, Object> props);
    }

    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    private static final int BUF_LEN = 1024;

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////
    private Database db;
    private MultipartReader _topReader;
    private MultipartDocumentReader _docReader;
    private BulkDownloaderDocument _onDocument;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    public RemoteBulkDownloaderRequest(HttpClientFactory factory,
                                       URL dbURL,
                                       boolean cancelable,
                                       List<RevisionInternal> revs,
                                       Database db,
                                       Map<String, Object> requestHeaders,
                                       BulkDownloaderDocument onDocument,
                                       RemoteRequestCompletion onCompletion)
            throws Exception {
        super(factory,
                "POST",
                new URL(buildRelativeURLString(dbURL, "/_bulk_get?revs=true&attachments=true")),
                cancelable,
                buildJSONBody(revs, db),
                requestHeaders,
                onCompletion);
        this.db = db;
        _onDocument = onDocument;
    }

    ////////////////////////////////////////////////////////////
    // Override methods
    ////////////////////////////////////////////////////////////

    /**
     * set headers
     */
    protected Request.Builder addHeaders(Request.Builder builder) {
        builder.addHeader("Content-Type", "application/json");
        builder.addHeader("Accept", "multipart/related");
        builder.addHeader("User-Agent", Manager.getUserAgent());
        builder.addHeader("X-Accept-Part-Encoding", "gzip");
        builder.addHeader("Accept-Encoding", "gzip, deflate");
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
                Log.v(TAG, "%s: BulkDownloader calling httpClient.execute, url: %s", this, url);
                call = httpClient.newCall(request);
                response = call.execute();
                Log.v(TAG, "%s: BulkDownloader called httpClient.execute, url: %s", this, url);

                storeCookie(response);

                // error
                if (response.code() >= 300) {
                    // conflict could be happen. So ERROR prints make developer confused.
                    if (response.code() == 409)
                        Log.w(TAG, "Got error status: %d for %s.  Reason: %s",
                                response.code(), request, response.message());
                    else
                        Log.i(TAG, "Got error status: %d for %s.  Reason: %s",
                                response.code(), request, response.message());
                    error = new RemoteRequestResponseException(response.code(), response.message());
                    RequestUtils.closeResponseBody(response);
                }
                // success
                else {
                    ResponseBody responseBody = response.body();
                    InputStream stream = response.body().byteStream();
                    try {
                        // decompress if contentEncoding is gzip
                        if (Utils.isGzip(response))
                            stream = new GZIPInputStream(stream);
                        MediaType type = responseBody.contentType();
                        if (type != null) {
                            // multipart
                            if (type.type().equals("multipart")) {
                                Log.v(TAG, "mediaType = %s", type);
                                _topReader = new MultipartReader(type.toString(), this);
                                byte[] buffer = new byte[BUF_LEN];
                                int nBytesRead;
                                while ((nBytesRead = stream.read(buffer)) != -1)
                                    _topReader.appendData(buffer, 0, nBytesRead);
                                _topReader.finished();
                            }
                            // JSON (non-multipart)
                            else {
                                Log.v(TAG, "contentTypeHeader is not multipart = %s",
                                        type.toString());
                                fullBody = Manager.getObjectMapper().readValue(
                                        stream, Object.class);
                            }
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

    ////////////////////////////////////////////////////////////
    // Public Methods
    ////////////////////////////////////////////////////////////
    public String toString() {
        return this.getClass().getName() + '[' + url.getPath() + ']';
    }

    ////////////////////////////////////////////////////////////
    // Implementations of MultipartReaderDelegate
    ////////////////////////////////////////////////////////////

    /**
     * This method is called when a part's headers have been parsed, before its data is parsed.
     */
    @Override
    public void startedPart(Map headers) {
        if (_docReader != null)
            throw new IllegalStateException("_docReader is already defined");
        Log.v(TAG, "%s: Starting new document; headers =%s", this, headers);
        _docReader = new MultipartDocumentReader(db);
        _docReader.setHeaders(headers);
        _docReader.startedPart(headers);
    }

    /**
     * This method is called to append data to a part's body.
     */
    @Override
    public void appendToPart(byte[] data) {
        appendToPart(data, 0, data.length);
    }

    @Override
    public void appendToPart(final byte[] data, int off, int len) {
        if (_docReader == null)
            throw new IllegalStateException("_docReader is not defined");
        _docReader.appendData(data, off, len);
    }

    /**
     * This method is called when a part is complete.
     */
    @Override
    public void finishedPart() {
        if (_docReader == null)
            throw new IllegalStateException("_docReader is not defined");
        _docReader.finish();
        _onDocument.onDocument(_docReader.getDocumentProperties());
        _docReader = null;
        Log.v(TAG, "%s: Finished document", this);
    }

    ////////////////////////////////////////////////////////////
    // Protected or Private Methods
    ////////////////////////////////////////////////////////////

    private static Map<String, Object> buildJSONBody(List<RevisionInternal> revs,
                                                     final Database db) {
        // Build up a JSON body describing what revisions we want:
        Collection<Map<String, Object>> keys = CollectionUtils.transform(
                revs, new CollectionUtils.Functor<RevisionInternal, Map<String, Object>>() {
                    public Map<String, Object> invoke(RevisionInternal source) {
                        AtomicBoolean hasAttachment = new AtomicBoolean(false);
                        List<String> attsSince = db.getPossibleAncestorRevisionIDs(
                                source, PullerInternal.MAX_NUMBER_OF_ATTS_SINCE, hasAttachment);
                        if (!hasAttachment.get() || attsSince.size() == 0)
                            attsSince = null;
                        Map<String, Object> mapped = new HashMap<String, Object>();
                        mapped.put("id", source.getDocID());
                        mapped.put("rev", source.getRevID());
                        mapped.put("atts_since", attsSince);
                        return mapped;
                    }
                });
        Map<String, Object> retval = new HashMap<String, Object>();
        retval.put("docs", keys);
        return retval;
    }

    @InterfaceAudience.Private
    private static String buildRelativeURLString(URL remote, String relativePath) {
        // the following code is a band-aid for a system problem in the codebase
        // where it is appending "relative paths" that start with a slash, eg:
        //     http://dotcom/db/ + /relpart == http://dotcom/db/relpart
        // which is not compatible with the way the java url concatonation works.
        String remoteUrlString = remote.toExternalForm();
        if (remoteUrlString.endsWith("/") && relativePath.startsWith("/"))
            remoteUrlString = remoteUrlString.substring(0, remoteUrlString.length() - 1);
        return remoteUrlString + relativePath;
    }
}
