package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.util.Log;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

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

        preemptivelySetAuthCredentials(httpClient);

        request.addHeader("Accept", "*/*");

        addRequestHeaders(request);

        executeRequest(httpClient, request);

    }

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
                Header contentTypeHeader = entity.getContentType();
                InputStream inputStream = null;

                if (contentTypeHeader != null
                        && contentTypeHeader.getValue().contains("multipart/related")) {

                    try {
                        MultipartDocumentReader reader = new MultipartDocumentReader(response, db);
                        reader.setContentType(contentTypeHeader.getValue());
                        inputStream = entity.getContent();

                        int bufLen = 1024;
                        byte[] buffer = new byte[bufLen];
                        int numBytesRead = 0;
                        while ( (numBytesRead = inputStream.read(buffer))!= -1 ) {
                            if (numBytesRead != bufLen) {
                                byte[] bufferToAppend = Arrays.copyOfRange(buffer, 0, numBytesRead);
                                reader.appendData(bufferToAppend);
                            }
                            else {
                                reader.appendData(buffer);
                            }
                        }

                        reader.finish();
                        fullBody = reader.getDocumentProperties();

                        respondWithResult(fullBody, error, response);

                    } finally {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                        }
                    }


                }
                else {
                    if (entity != null) {
                        try {
                            inputStream = entity.getContent();
                            fullBody = Manager.getObjectMapper().readValue(inputStream,
                                    Object.class);
                            respondWithResult(fullBody, error, response);
                        } finally {
                            try {
                                inputStream.close();
                            } catch (IOException e) {
                            }
                        }
                    }

                }
            }
        } catch (IOException e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "%s: io exception", e, this);
            error = e;
            respondWithResult(fullBody, e, response);
        } catch (Exception e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "%s: executeRequest() Exception: ", e, this);
            error = e;
            respondWithResult(fullBody, e, response);
        } finally {
            Log.e(Log.TAG_REMOTE_REQUEST, "%s: executeRequest() finally", this);
        }
    }

}
