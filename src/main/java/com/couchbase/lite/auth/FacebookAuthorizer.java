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
package com.couchbase.lite.auth;

import com.couchbase.lite.util.Log;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import okhttp3.Headers;

/**
 * Authenticator impl that knows how to do facebook auth
 *
 * @exclude
 */
public class FacebookAuthorizer extends BaseAuthorizer implements SessionCookieAuthorizer {

    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    private static final String TAG = Log.TAG_SYNC;

    private static final String kLoginParamAccessToken = "access_token";

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    private static Map<List<String>, String> sRegisteredTokens
            = Collections.synchronizedMap(new HashMap<List<String>, String>());
    private String email;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////
    public FacebookAuthorizer(String email) {
        if (email == null)
            throw new IllegalArgumentException("email is null");
        this.email = email;
    }

    ////////////////////////////////////////////////////////////
    // Implementation of SessionCookieAuthorizer (LoginAuthorizer)
    ////////////////////////////////////////////////////////////
    @Override
    public List<Object> loginRequest() {
        String token = token();
        if (token == null)
            return null;
        Map<String, String> loginParams = new HashMap<String, String>();
        loginParams.put(kLoginParamAccessToken, token);
        return Arrays.asList("POST", "_facebook", loginParams);
    }

    @Override
    public void loginResponse(Object jsonResponse,
                              Headers headers,
                              Throwable error,
                              ContinuationBlock block) {
        // @optional
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean implementedLoginResponse() {
        return false;
    }

    ////////////////////////////////////////////////////////////
    // Public methods
    ////////////////////////////////////////////////////////////
    public static boolean registerToken(String token, String email, URL site) {
        List<String> key = new ArrayList<String>();
        key.add(email);
        key.add(site.toExternalForm().toLowerCase());
        sRegisteredTokens.put(key, token);
        return true;
    }

    ////////////////////////////////////////////////////////////
    // Protected & Private methods
    ////////////////////////////////////////////////////////////

    private String token() {
        List<String> key = new ArrayList<String>();
        key.add(email);
        key.add(getRemoteURL().toExternalForm().toLowerCase());
        Log.v(TAG, "FacebookAuthorizer looking up key: %s from list of access tokens", key);
        return sRegisteredTokens.get(key);
    }
}
