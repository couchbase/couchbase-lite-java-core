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

import com.couchbase.lite.auth.Authenticator;
import com.couchbase.lite.auth.CredentialAuthorizer;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.URIUtils;

import java.net.URL;

import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * Created by hideki on 5/19/16.
 */
public class RequestUtils {
    public final static String TAG = Log.TAG_SYNC;

    /*package*/
    static Request.Builder preemptivelySetAuthCredentials(
            Request.Builder builder, URL url, Authenticator authenticator) {
        boolean isUrlBased = false;
        String userInfo = url.getUserInfo();
        if (userInfo != null) {
            isUrlBased = true;
        } else {
            if (authenticator != null && authenticator instanceof CredentialAuthorizer) {
                userInfo = ((CredentialAuthorizer) authenticator).authUserInfo();
            }
        }
        return preemptivelySetAuthCredentials(builder, userInfo, isUrlBased);
    }

    // Handling authentication
    // https://github.com/square/okhttp/wiki/Recipes#handling-authentication
    /*package*/
    static Request.Builder preemptivelySetAuthCredentials(
            Request.Builder builder, String userInfo, boolean isUrlBased) {

        if (userInfo == null)
            return builder;

        if (!userInfo.contains(":") || ":".equals(userInfo.trim())) {
            Log.w(TAG, "RemoteRequest Unable to parse user info, not setting credentials");
            return builder;
        }

        String[] userInfoElements = userInfo.split(":");
        String username = isUrlBased ? URIUtils.decode(userInfoElements[0]) : userInfoElements[0];
        String password = "";
        if (userInfoElements.length >= 2)
            password = isUrlBased ? URIUtils.decode(userInfoElements[1]) : userInfoElements[1];
        String credential = Credentials.basic(username, password);
        return builder.addHeader("Authorization", credential);
    }

    static void closeResponseBody(Response response) {
        if (response != null) {
            ResponseBody body = response.body();
            if (body != null)
                body.close();
        }
    }
}
