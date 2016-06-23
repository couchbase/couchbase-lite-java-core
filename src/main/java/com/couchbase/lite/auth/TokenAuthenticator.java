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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import okhttp3.Headers;

/**
 * Authenticator impl that knows how to do token based auth
 *
 * @exclude
 */
public class TokenAuthenticator extends BaseAuthorizer implements SessionCookieAuthorizer {

    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    public static final String TAG = Log.TAG_SYNC;

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////
    private String loginPath;
    private Map<String, String> loginParams;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////
    public TokenAuthenticator(String loginPath, Map<String, String> params) {
        this.loginPath = loginPath;
        this.loginParams = params;
    }

    ////////////////////////////////////////////////////////////
    // Implementation of SessionCookieAuthorizer (LoginAuthorizer)
    ////////////////////////////////////////////////////////////

    @Override
    public List<Object> loginRequest() {
        if (loginParams == null)
            return null;
        return Arrays.asList("POST", loginPath, loginParams);
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
}
