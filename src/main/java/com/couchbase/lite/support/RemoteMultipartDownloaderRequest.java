package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
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
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.zip.GZIPInputStream;

public class RemoteMultipartDownloaderRequest extends RemoteRequest {

    private Database db;

    public RemoteMultipartDownloaderRequest(ScheduledExecutorService workExecutor,
                                            HttpClientFactory clientFactory, String method, URL url,
                                            Object body, Database db, Map<String, Object> requestHeaders, RemoteRequestCompletionBlock onCompletion) {
        super(workExecutor, clientFactory, method, url, body, db, requestHeaders, onCompletion);
        this.db = db;
    }

    @Override
    public void run() {
        HttpClient httpClient = clientFactory.getHttpClient();
        try {
            preemptivelySetAuthCredentials(httpClient);
            request.addHeader("Accept", "multipart/related, application/json");
            request.addHeader("X-Accept-Part-Encoding", "gzip");
            request.addHeader("User-Agent", Manager.getUserAgent());
            request.addHeader("Accept-Encoding", "gzip, deflate");
            addRequestHeaders(request);
            executeRequest(httpClient, request);
        } finally {
            // shutdown connection manager (close all connections)
            if (httpClient != null && httpClient.getConnectionManager() != null)
                httpClient.getConnectionManager().shutdown();
        }
    }

    private static final int BUF_LEN = 1024;
    protected void executeRequest(HttpClient httpClient, HttpUriRequest request) {
        Object fullBody = null;
        Throwable error = null;
        HttpResponse response = null;

        try {
            if (request.isAborted()) {
                respondWithResult(fullBody, new Exception(String.format("%s: Request %s has been aborted", this, request)), response);
                return;
            }

            response = httpClient.execute(request);

            try {
                // add in cookies to global store
                if (httpClient instanceof DefaultHttpClient) {
                    DefaultHttpClient defaultHttpClient = (DefaultHttpClient)httpClient;
                    this.clientFactory.addCookies(defaultHttpClient.getCookieStore().getCookies());
                }
            } catch (Exception e) {
                Log.e(Log.TAG_REMOTE_REQUEST, "Unable to add in cookies to global store", e);
            }

            StatusLine status = response.getStatusLine();
            if (status.getStatusCode() >= 300) {
                Log.e(Log.TAG_REMOTE_REQUEST, "Got error status: %d for %s.  Reason: %s", status.getStatusCode(), request, status.getReasonPhrase());
                error = new HttpResponseException(status.getStatusCode(),
                        status.getReasonPhrase());
                respondWithResult(fullBody, error, response);
            } else {
                HttpEntity entity = response.getEntity();
                try {
                    if (entity != null) {
                        InputStream inputStream = entity.getContent();
                        try {
                            // decompress if contentEncoding is gzip
                            if (Utils.isGzip(entity))
                                inputStream = new GZIPInputStream(inputStream);

                            Header contentTypeHeader = entity.getContentType();
                            if (contentTypeHeader != null) {
                                // multipart
                                if (contentTypeHeader.getValue().contains("multipart/related")) {
                                    MultipartDocumentReader reader = new MultipartDocumentReader(db);
                                    reader.setHeaders(Utils.headersToMap(response.getAllHeaders()));
                                    byte[] buffer = new byte[BUF_LEN];
                                    int numBytesRead = 0;
                                    while ((numBytesRead = inputStream.read(buffer)) != -1) {
                                        reader.appendData(buffer, 0, numBytesRead);
                                    }
                                    reader.finish();
                                    fullBody = reader.getDocumentProperties();
                                    respondWithResult(fullBody, error, response);
                                }
                                // non-multipart
                                else {
                                    fullBody = Manager.getObjectMapper().readValue(inputStream, Object.class);
                                    respondWithResult(fullBody, error, response);
                                }
                            }
                        }
                        finally {
                            try {
                                if (inputStream != null) {
                                    inputStream.close();
                                }
                            } catch (IOException e) {
                            }
                        }
                    }
                }
                finally{
                    if (entity != null) {
                        try {
                            entity.consumeContent();
                        } catch (IOException e) {
                        }
                    }
                }
            }
        } catch (IOException e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "%s: io exception", e, this);
            respondWithResult(fullBody, e, response);
        } catch (Exception e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "%s: executeRequest() Exception: ", e, this);
            respondWithResult(fullBody, e, response);
        } finally {
            Log.d(Log.TAG_REMOTE_REQUEST, "%s: executeRequest() finally", this);
        }
    }
}
