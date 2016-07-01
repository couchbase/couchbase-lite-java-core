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

import java.net.URL;

/**
 * A Login continuation callback that will be passed to your OIDCLoginCallback. You should call this
 * callback when your login UI completes, so that Couchbase Lite's replicator can continue or stop.
 */
public interface OIDCLoginContinuation {
    /**
     * Callback method that should be called when the login UI completes, so that Couchbase Lite's
     * replicator can continue or stop.
     * @param redirectedURL The authentication URL to which the WebView was redirected.
     *                      It will have the same host and path as the redirectURL passed to your
     *                      login callback, plus extra query parameters appended.
     *                      If login did not complete successfully, pass null.
     * @param error         If the login UI failed, pass the error here (and a nil authURL.)
     *                      As a special case, if both error and authURL are null, it's
     *                      interpreted as an authentication-canceled error.
     */
    void callback(URL redirectedURL, Throwable error);
}