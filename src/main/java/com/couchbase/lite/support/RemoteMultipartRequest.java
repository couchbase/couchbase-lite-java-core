package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.util.Log;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.mime.MultipartEntity;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class RemoteMultipartRequest extends RemoteRequest {

    private MultipartEntity multiPart;

    public RemoteMultipartRequest(ScheduledExecutorService workExecutor,
                                  HttpClientFactory clientFactory, String method, URL url,
                                  MultipartEntity multiPart, Database db, Map<String, Object> requestHeaders, RemoteRequestCompletionBlock onCompletion) {
        super(workExecutor, clientFactory, method, url, null, db, requestHeaders, onCompletion);
        this.multiPart = multiPart;
    }

    @Override
    public void run() {

        try {
            HttpClient httpClient = clientFactory.getHttpClient();

            preemptivelySetAuthCredentials(httpClient);

            HttpUriRequest request = null;
            if (method.equalsIgnoreCase("PUT")) {
                HttpPut putRequest = new HttpPut(url.toExternalForm());
                putRequest.setEntity(multiPart);
                request = putRequest;

            } else if (method.equalsIgnoreCase("POST")) {
                HttpPost postRequest = new HttpPost(url.toExternalForm());
                postRequest.setEntity(multiPart);
                request = postRequest;
            } else {
                throw new IllegalArgumentException("Invalid request method: " + method);
            }

            request.addHeader("Accept", "*/*");

            executeRequest(httpClient, request);

        } catch (Exception e) {
            Log.e(Log.TAG_REMOTE_REQUEST, "caught and rethrowing unexpected exception: ", e);
            throw new RuntimeException(e);
        }

    }


}
