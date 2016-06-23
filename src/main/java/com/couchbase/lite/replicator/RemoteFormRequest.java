//
// Copyright (c) 2016 Couchbase, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
//
package com.couchbase.lite.replicator;

import com.couchbase.lite.support.HttpClientFactory;

import java.net.URL;
import java.util.Iterator;
import java.util.Map;

import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;

/**
 * Created by hideki on 6/21/16.
 */
public class RemoteFormRequest extends RemoteRequest {

    public RemoteFormRequest(HttpClientFactory factory, String method, URL url, boolean cancelable,
                             Map<String, String> formBody, Map<String, Object> requestHeaders,
                             RemoteRequestCompletion onCompletion) {
        super(factory, method, url, cancelable, formBody, requestHeaders, onCompletion);
    }

    /**
     * set request body
     */
    protected Request.Builder setBody(Request.Builder builder) {
        if (body != null) {
            RequestBody requestBody = null;
            if (requestBody == null) {
                FormBody.Builder fbBuilder = new FormBody.Builder();
                Iterator<String> itr = body.keySet().iterator();
                while (itr.hasNext()) {
                    String key = itr.next();
                    fbBuilder.add(key, (String) body.get(key));
                }
                requestBody = fbBuilder.build();
            }

            if ("PUT".equalsIgnoreCase(method))
                builder.put(requestBody);
            else if ("POST".equalsIgnoreCase(method))
                builder.post(requestBody);
        }
        return builder;
    }
}
