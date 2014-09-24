package com.couchbase.lite.support;


import org.apache.http.HttpResponse;

public interface RemoteRequestCompletionBlock {

    public void onCompletion(HttpResponse httpResponse, Object result, Throwable e);

}
