package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.org.apache.http.entity.mime.MultipartEntity;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class RemoteMultipartRequest extends RemoteRequest {

    private MultipartEntity multiPart = null;

    public RemoteMultipartRequest(ScheduledExecutorService workExecutor,
                                  HttpClientFactory clientFactory, String method, URL url,
                                  MultipartEntity multiPart, Database db, Map<String, Object> requestHeaders, RemoteRequestCompletionBlock onCompletion) {
        super(workExecutor, clientFactory, method, url, null, db, requestHeaders, onCompletion);
        this.multiPart = multiPart;
    }

    @Override
    public void run() {
        HttpClient httpClient = clientFactory.getHttpClient();
        try {
            preemptivelySetAuthCredentials(httpClient);

            HttpUriRequest request = null;
            if ("PUT".equalsIgnoreCase(method)) {
                HttpPut putRequest = new HttpPut(url.toExternalForm());
                putRequest.setEntity(multiPart);
                request = putRequest;
            } else if ("POST".equalsIgnoreCase(method)) {
                HttpPost postRequest = new HttpPost(url.toExternalForm());
                postRequest.setEntity(multiPart);
                request = postRequest;
            } else {
                throw new IllegalArgumentException("Invalid request method: " + method);
            }

            addRequestHeaders(request);
            request.addHeader("Accept", "*/*");

            executeRequest(httpClient, request);
        } finally {
            // shutdown connection manager (close all connections)
            if (httpClient != null && httpClient.getConnectionManager() != null)
                httpClient.getConnectionManager().shutdown();
        }
    }
}
