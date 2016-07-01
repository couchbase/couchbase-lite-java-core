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
 * A login callback block given when creating an OpenID Connect authenticator. The callback method
 * will be called when the OpenID Connect login flow requires the user to authenticate
 * with the Originating Party (OP), the site at which they have an account.
 *
 * In general when the callback method is called, the callback method should open a modal
 * web view starting at the given loginURL, then return. Just make sure you hold onto
 * the loginContinuation object, because you MUST call it later, or the replicator will never
 * finish logging in!
 *
 * Wait for the web view to redirect to a URL whose host and path are the same as the given
 * redirectURL (the query string after the path will be different, though.) Instead of following
 * the redirect, close the web view and call the given continuation block with the redirected
 * URL (and a null error.)
 *
 * Your modal web view UI should provide a way for the user to cancel, probably by adding a
 * Cancel button outside the web view. If the user cancels, call the continuation block with
 * a null URL and a null error.
 *
 * If something else goes wrong, like an error loading the login page in the web view, call the
 * login continuation's callback with that error and a null URL
 */
public interface OIDCLoginCallback {
    /**
     * A callback method called when the OpenID Connect Login flow requires the user to authenticate
     * with the Originating Party (OP).
     * @param loginURL          The url given to the web view to start the authentication flow
     *                          with the Originating party.
     * @param redirectURL       The redirect url used for determining when authentication with
     *                          the Originating party is completed and when the login continuation
     *                          should be processed. Technically you need to wait for the web view
     *                          to redirect to a URL whose host and path are the same as the given
     *                          redirectURL (the query string after the path will be different)
     *                          Instead of following the redirect, close the web view and call
     *                          the given login continuation's callback method with the redirected
     *                          URL (and a null error.)
     * @param loginContinuation The login continuation's callback to be called after receiving the
     *                          redirect url or an error from the web view.
     */
    void callback(URL loginURL, URL redirectURL, OIDCLoginContinuation loginContinuation);
}