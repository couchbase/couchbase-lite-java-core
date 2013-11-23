package com.couchbase.cblite.testapp.tests;

import android.util.Log;

import com.couchbase.cblite.CBLServer;
import com.couchbase.cblite.R;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseFactory;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockHttpClient2 implements org.apache.http.client.HttpClient {

    @Override
    public HttpParams getParams() {
        return null;
    }

    @Override
    public ClientConnectionManager getConnectionManager() {
        return null;
    }

    @Override
    public HttpResponse execute(HttpUriRequest httpUriRequest) throws IOException, ClientProtocolException {

        if (httpUriRequest.getURI().getPath().contains("_revs_diff")) {
            return fakeRevsDiff(httpUriRequest);
        } else if (httpUriRequest.getURI().getPath().contains("_bulk_docs")) {
            return fakeBulkDocs(httpUriRequest);
        } else if (httpUriRequest.getURI().getPath().contains("_local")) {
            return fakeLocalDocumentUpdate(httpUriRequest);
        } else {
            throw new IOException("Mock http client does not know how to handle this request");
        }

    }

    /*

    Request:

        {
            "lastSequence": "3"
        }

    Response:

        {
            "id": "_local/49b8682aa71e34628ebd9b3a8764fdbcfc9b03a6",
            "ok": true,
            "rev": "0-1"
        }

     */
    public HttpResponse fakeLocalDocumentUpdate(HttpUriRequest httpUriRequest) throws IOException, ClientProtocolException {

        Map<String, Object> responseMap = new HashMap<String, Object>();
        responseMap.put("id", "_local/49b8682aa71e34628ebd9b3a8764fdbcfc9b03a6");
        responseMap.put("ok", true);
        responseMap.put("rev", "0-1");

        HttpResponse response = generateHttpResponseObject(responseMap);
        return response;

    }

    /*

    Request:

    {
        "new_edits": false,
        "docs": [{
            "_rev": "2-e776a593-6b61-44ee-b51a-0bdf205c9e13",
            "foo": 1,
            "_id": "doc1-1384988871931",
            "_revisions": {
                "start": 2,
                "ids": ["e776a593-6b61-44ee-b51a-0bdf205c9e13", "38af0cab-d397-4b68-a59e-67c341f98dc4"]
            }
        }, {

        ...

        }
    }

    Response:

    [{
        "id": "doc1-1384988871931",
        "rev": "2-e776a593-6b61-44ee-b51a-0bdf205c9e13"
    }, {
       ...
    }]

     */
    public HttpResponse fakeBulkDocs(HttpUriRequest httpUriRequest) throws IOException, ClientProtocolException {

        Map<String, Object> jsonMap = getJsonMapFromRequest((HttpPost) httpUriRequest);

        List<Map<String, Object>> responseList = new ArrayList<Map<String, Object>>();

        ArrayList<Map<String, Object>> docs = (ArrayList) jsonMap.get("docs");
        for (Map<String, Object> doc : docs) {
            Map<String, Object> responseListItem = new HashMap<String, Object>();
            responseListItem.put("id", doc.get("_id"));
            responseListItem.put("rev", doc.get("_rev"));
            responseList.add(responseListItem);
        }

        HttpResponse response = generateHttpResponseObject(responseList);
        return response;

    }

    /*

    Transform Request JSON:

        {
            "doc2-1384988871931": ["1-b52e6d59-4151-4802-92fb-7e34ceff1e92"],
            "doc1-1384988871931": ["2-e776a593-6b61-44ee-b51a-0bdf205c9e13"]
        }

    Into Response JSON:

        {
            "doc1-1384988871931": {
                "missing": ["2-e776a593-6b61-44ee-b51a-0bdf205c9e13"]
            },
            "doc2-1384988871931": {
                "missing": ["1-b52e6d59-4151-4802-92fb-7e34ceff1e92"]
            }
        }

     */
    public HttpResponse fakeRevsDiff(HttpUriRequest httpUriRequest) throws IOException, ClientProtocolException {

        Map<String, Object> jsonMap = getJsonMapFromRequest((HttpPost) httpUriRequest);

        Map<String, Object> responseMap = new HashMap<String, Object>();
        for (String key : jsonMap.keySet()) {
            ArrayList value = (ArrayList) jsonMap.get(key);
            Map<String, Object> missingMap = new HashMap<String, Object>();
            missingMap.put("missing", value);
            responseMap.put(key, missingMap);
        }

        HttpResponse response = generateHttpResponseObject(responseMap);
        return response;

    }

    private HttpResponse generateHttpResponseObject(Object o) throws IOException {
        DefaultHttpResponseFactory responseFactory = new DefaultHttpResponseFactory();
        BasicStatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK");
        HttpResponse response = responseFactory.newHttpResponse(statusLine, null);
        byte[] responseBytes = CBLServer.getObjectMapper().writeValueAsBytes(o);
        response.setEntity(new ByteArrayEntity(responseBytes));
        return response;
    }

    private Map<String, Object> getJsonMapFromRequest(HttpPost httpUriRequest) throws IOException {
        HttpPost post = (HttpPost) httpUriRequest;
        InputStream is =  post.getEntity().getContent();
        return CBLServer.getObjectMapper().readValue(is, Map.class);
    }

    @Override
    public HttpResponse execute(HttpUriRequest httpUriRequest, HttpContext httpContext) throws IOException, ClientProtocolException {
        return null;
    }

    @Override
    public HttpResponse execute(HttpHost httpHost, HttpRequest httpRequest) throws IOException, ClientProtocolException {
        return null;
    }

    @Override
    public HttpResponse execute(HttpHost httpHost, HttpRequest httpRequest, HttpContext httpContext) throws IOException, ClientProtocolException {
        return null;
    }

    @Override
    public <T> T execute(HttpUriRequest httpUriRequest, ResponseHandler<? extends T> responseHandler) throws IOException, ClientProtocolException {
        return null;
    }

    @Override
    public <T> T execute(HttpUriRequest httpUriRequest, ResponseHandler<? extends T> responseHandler, HttpContext httpContext) throws IOException, ClientProtocolException {
        return null;
    }

    @Override
    public <T> T execute(HttpHost httpHost, HttpRequest httpRequest, ResponseHandler<? extends T> responseHandler) throws IOException, ClientProtocolException {
        return null;
    }

    @Override
    public <T> T execute(HttpHost httpHost, HttpRequest httpRequest, ResponseHandler<? extends T> responseHandler, HttpContext httpContext) throws IOException, ClientProtocolException {
        return null;
    }





}
