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

import okhttp3.Request;

/**
 * Authorizer that adds custom headers to an HTTP request
 */
public interface CustomHeadersAuthorizer extends Authorizer {
    /**
     * May add a header to the request (usually "Authorization:") to convey the authorization token.
     *
     * @param builder The URL request to authenticate.
     * @return true if it added authorization.
     */
    boolean authorizeURLRequest(Request.Builder builder);
}
