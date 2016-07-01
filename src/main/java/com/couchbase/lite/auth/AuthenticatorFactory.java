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

import java.util.HashMap;
import java.util.Map;

/**
 * A factory to create instances of supported Authenticators.
 */
public class AuthenticatorFactory {

    /*
     * Creates an Authenticator that knows how to do Basic authentication.
     *
     * @param username      username
     * @param password      password
     */
    public static Authenticator createBasicAuthenticator(String username, String password) {
        return new PasswordAuthorizer(username, password);
    }

    /*
     * Creates an Authenticator that knows how to do Facebook authentication.
     *
     * @param token      Facebook access token
     */
    public static Authenticator createFacebookAuthenticator(String token) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("access_token", token);
        return new TokenAuthenticator("_facebook", params);
    }

    /*
     * Creates an Authenticator that knows how to do Persona authentication.
     *
     * @param assertion     Persona Assertion
     */
    public static Authenticator createPersonaAuthenticator(String assertion) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("assertion", assertion);
        return new TokenAuthenticator("_persona", params);
    }

    /*
     * Creates an Authenticator that knows how to do OpenID Connect authentication.
     */
    public static Authenticator createOpenIDConnectAuthenticator(
            OIDCLoginCallback callback, TokenStore tokenStore) {
        return new OpenIDConnectAuthorizer(callback, tokenStore);
    }
}