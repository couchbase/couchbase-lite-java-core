package com.couchbase.lite.replicator;

import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.MultipartDocumentReader;
import com.couchbase.lite.support.MultipartReader;
import com.couchbase.lite.support.MultipartReaderDelegate;
import com.couchbase.lite.support.RemoteRequest;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

/**
 * A special type of RemoteRequest that knows how to call the _bulk_get endpoint.
 *
 * @exclude
 */
@InterfaceAudience.Private
public class BulkDownloader extends RemoteRequest implements MultipartReaderDelegate {

    private Database _db;
    private MultipartReader _topReader;
    private MultipartDocumentReader _docReader;
    private BulkDownloaderDocumentBlock _onDocument;

    public BulkDownloader(ScheduledExecutorService workExecutor,
                          HttpClientFactory clientFactory,
                          URL dbURL,
                          List<RevisionInternal> revs,
                          Database database,
                          Map<String, Object> requestHeaders,
                          BulkDownloaderDocumentBlock onDocument,
                          RemoteRequestCompletionBlock onCompletion) throws Exception {

        super(workExecutor,
                clientFactory,
                "POST",
                new URL(buildRelativeURLString(dbURL, "/_bulk_get?revs=true&attachments=true")),
                helperMethod(revs, database),
                database,
                requestHeaders,
                onCompletion);

        _db = database;
        _onDocument = onDocument;

    }

    @Override
    public void run() {
        HttpClient httpClient = clientFactory.getHttpClient();

        preemptivelySetAuthCredentials(httpClient);

        request.addHeader("Content-Type", "application/json");
        request.addHeader("Accept", "multipart/related");
        request.addHeader("X-Accept-Part-Encoding", "gzip");
        request.addHeader("User-Agent", Manager.USER_AGENT);
        request.addHeader("Accept-Encoding", "gzip, deflate");

        addRequestHeaders(request);

        setBody(request);

        executeRequest(httpClient, request);
    }

    public String toString() {
        return this.getClass().getName() + "[" + url.getPath() + "]";
    }

    private static final int BUF_LEN = 1024;

    @Override
    protected void executeRequest(HttpClient httpClient, HttpUriRequest request) {
        Object fullBody = null;
        Throwable error = null;
        HttpResponse response = null;

        try {
            if (request.isAborted()) {
                respondWithResult(fullBody, new Exception(
                        String.format("%s: Request %s has been aborted", this, request)), response);
                return;
            }

            response = httpClient.execute(request);

            try {
                // add in cookies to global store
                if (httpClient instanceof DefaultHttpClient) {
                    DefaultHttpClient defaultHttpClient = (DefaultHttpClient) httpClient;
                    clientFactory.addCookies(defaultHttpClient.getCookieStore().getCookies());
                }
            } catch (Exception e) {
                Log.e(Log.TAG_REMOTE_REQUEST, "Unable to add in cookies to global store", e);
            }

            StatusLine status = response.getStatusLine();
            if (status.getStatusCode() >= 300) {
                Log.e(Log.TAG_REMOTE_REQUEST, "Got error status: %d for %s.  Reason: %s",
                        status.getStatusCode(), request, status.getReasonPhrase());
                error = new HttpResponseException(status.getStatusCode(),
                        status.getReasonPhrase());
            } else {
                HttpEntity entity = null;
                try {
                    entity = response.getEntity();
                    if (entity != null) {
                        InputStream inputStream = null;
                        try {
                            inputStream = entity.getContent();

                            Header contentTypeHeader = entity.getContentType();
                            if (contentTypeHeader != null) {
                                // multipart
                                if (contentTypeHeader.getValue().contains("multipart/")) {
                                    Log.v(Log.TAG_SYNC, "contentTypeHeader = %s",
                                            contentTypeHeader.getValue());
                                    _topReader = new MultipartReader(contentTypeHeader.getValue(), this);
                                    byte[] buffer = new byte[BUF_LEN];
                                    int numBytesRead = 0;
                                    while ((numBytesRead = inputStream.read(buffer)) != -1) {
                                        _topReader.appendData(buffer, 0, numBytesRead);
                                    }
                                    _topReader.finished();
                                    respondWithResult(fullBody, error, response);
                                }
                                // non-multipart
                                else {
                                    Log.v(Log.TAG_SYNC, "contentTypeHeader is not multipart = %s",
                                            contentTypeHeader.getValue());
                                    GZIPInputStream gzipStream = null;
                                    try {
                                        // decompress if contentEncoding is gzip
                                        if (Utils.isGzip(entity)) {
                                            gzipStream = new GZIPInputStream(inputStream);
                                            fullBody = Manager.getObjectMapper().readValue(
                                                    gzipStream, Object.class);
                                        } else {
                                            fullBody = Manager.getObjectMapper().readValue(
                                                    inputStream, Object.class);
                                        }
                                        respondWithResult(fullBody, error, response);
                                    } finally {
                                        try {
                                            if (gzipStream != null) {
                                                gzipStream.close();
                                            }
                                        } catch (IOException e) {
                                        }
                                        gzipStream = null;
                                    }
                                }
                            }
                        } finally {
                            try {
                                if (inputStream != null) {
                                    inputStream.close();
                                }
                            } catch (IOException e) {
                            }
                            inputStream = null;
                        }
                    }
                } finally {
                    if (entity != null) {
                        try {
                            entity.consumeContent();
                        } catch (IOException e) {
                        }
                    }
                    entity = null;
                }
            }
        } catch (IOException e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "io exception", e);
            error = e;
        } catch (Exception e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "%s: executeRequest() Exception: ", e, this);
            error = e;
        } finally {
            Log.v(Log.TAG_SYNC, "%s: BulkDownloader finally block.  url: %s", this, url);
        }

        Log.v(Log.TAG_SYNC, "%s: BulkDownloader calling respondWithResult.  url: %s, error: %s", this, url, error);
        respondWithResult(fullBody, error, response);
    }

    /**
     * This method is called when a part's headers have been parsed, before its data is parsed.
     */

    public void startedPart(Map headers) {
        if (_docReader != null) {
            throw new IllegalStateException("_docReader is already defined");
        }
        Log.v(Log.TAG_SYNC, "%s: Starting new document; headers =%s", this, headers);
        Log.v(Log.TAG_SYNC, "%s: Starting new document; ID=%s", this, headers.get("X-Doc-Id"));
        _docReader = new MultipartDocumentReader(_db);
        _docReader.setHeaders(headers);
        _docReader.startedPart(headers);
    }

    /**
     * This method is called to append data to a part's body.
     */

    public void appendToPart(byte[] data) {
        appendToPart(data, 0, data.length);
    }

    public void appendToPart(final byte[] data, int off, int len) {
        if (_docReader == null) {
            throw new IllegalStateException("_docReader is not defined");
        }
        _docReader.appendData(data, off, len);
    }

    /**
     * This method is called when a part is complete.
     */
    public void finishedPart() {
        Log.v(Log.TAG_SYNC, "%s: Finished document", this);
        if (_docReader == null) {
            throw new IllegalStateException("_docReader is not defined");
        }

        _docReader.finish();
        _onDocument.onDocument(_docReader.getDocumentProperties());
        _docReader = null;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public interface BulkDownloaderDocumentBlock {
        public void onDocument(Map<String, Object> props);
    }

    private static Map<String, Object> helperMethod(List<RevisionInternal> revs, final Database database) {

        // Build up a JSON body describing what revisions we want:
        Collection<Map<String, Object>> keys = CollectionUtils.transform(revs, new CollectionUtils.Functor<RevisionInternal, Map<String, Object>>() {

            public Map<String, Object> invoke(RevisionInternal source) {
                AtomicBoolean hasAttachment = new AtomicBoolean(false);
                List<String> attsSince = database.getPossibleAncestorRevisionIDs(source, PullerInternal.MAX_NUMBER_OF_ATTS_SINCE, hasAttachment);
                if (!hasAttachment.get() || attsSince.size() == 0) {
                    attsSince = null;
                }
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
        if (remoteUrlString.endsWith("/") && relativePath.startsWith("/")) {
            remoteUrlString = remoteUrlString.substring(0, remoteUrlString.length() - 1);
        }
        return remoteUrlString + relativePath;
    }
}
