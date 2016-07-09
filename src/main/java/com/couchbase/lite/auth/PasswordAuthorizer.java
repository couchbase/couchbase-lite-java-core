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

import okhttp3.Credentials;
import okhttp3.Request;

/**
 * Authenticator impl that knows how to do basic auth
 *
 * @note CBLCredentialAuthorizer is not implmented.
 * @exclude
 */
public class PasswordAuthorizer extends BaseAuthorizer
        implements CustomHeadersAuthorizer, CredentialAuthorizer {

    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    public static final String TAG = Log.TAG_SYNC;

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    private String username;
    private String password;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////
    public PasswordAuthorizer(String username, String password) {
        this.username = username;
        this.password = password;
    }

    ////////////////////////////////////////////////////////////
    // Implementation of CustomHeadersAuthorizer
    ////////////////////////////////////////////////////////////
    @Override
    public boolean authorizeURLRequest(Request.Builder builder) {
        if (authUserInfo() == null)
            return false;
        String credential = Credentials.basic(username, password);
        builder.addHeader("Authorization", credential);
        return true;
    }

    ////////////////////////////////////////////////////////////
    // Implementation of CredentialAuthorizer
    ////////////////////////////////////////////////////////////
    @Override
    public String authUserInfo() {
        if (this.username != null && this.password != null) {
            return this.username + ':' + this.password;
        }
        return null;
    }
    ////////////////////////////////////////////////////////////
    // Implementation of Authorizer
    ////////////////////////////////////////////////////////////

    @Override
    public boolean removeStoredCredentials() {
        this.username = null;
        this.password = null;
        return true;
    }

    @Override
    public String getUsername() {
        // @optional
        return username;
    }
}
