/**
 * Copyright (c) 2016 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.couchbase.lite.auth;

import java.net.URL;
import java.util.Map;

/**
 * Authenticator impl that knows how to do basic auth
 *
 * @exclude
 */
public class BasicAuthenticator extends AuthenticatorImpl {
    private String username;
    private String password;

    public BasicAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public boolean usesCookieBasedLogin() {
        return true;
    }

    @Override
    public String authUserInfo() {
        if (this.username != null && this.password != null) {
            return this.username + ':' + this.password;
        }
        return super.authUserInfo();
    }

    @Override
    public String loginPathForSite(URL site) {
        return "/_session";
    }

    @Override
    public Map<String, String> loginParametersForSite(URL site) {
        // This method has different implementation from the iOS's.
        // It is safe to return NULL as the method is not called
        // when Basic Authenticator is used. Also theoretically, the
        // standard Basic Auth doesn't add any additional parameters
        // to the login url.
        return null;
    }
}
