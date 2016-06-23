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

import java.util.List;

import okhttp3.Headers;

/**
 * Created by hideki on 6/14/16.
 */
public interface LoginAuthorizer extends Authorizer {

    /**
     * Returns the HTTP method, URL, and body of the login request to send, or nil for no login.
     *
     * @return An array of the form @[method, path, body]. The path may be absolute, or relative to
     * the authorizer's remote URL. Return nil to skip the login.
     */
    List<Object> loginRequest();

    /**
     * The replicator calls this method with the response to the login request. It then waits for the
     * authorizer to call the continuation callback, before proceeding.
     *
     * @param jsonResponse The parsed JSON body of the response.
     * @param headers      The HTTP response headers.
     * @param error        The error, if the request failed.
     * @param block        The authorizer must call this block at some future time. If the
     *                     `loginAgain` parameter is YES, the login will be repeated, with another call to
     *                     -loginRequestForSite:. If the NSError* parameter is non-nil, replication stops.
     * @optional optinal method for CBL iOS
     */
    void loginResponse(Object jsonResponse,
                       Headers headers,
                       Throwable error,
                       ContinuationBlock block);

    boolean implementedLoginResponse();

    interface ContinuationBlock {
        void call(boolean loginAgain, final Throwable error);
    }
}
