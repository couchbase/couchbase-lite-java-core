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

/**
 * @deprecated Class name was changed to PasswordAuthorizer. To be consistent with other platforms,
 * please use `Authenticator AuthenticatorFactory.createBasicAuthenticator(String username, String password)`
 * to create authenticator for Basic authentication.
 */
@Deprecated
public class BasicAuthenticator extends PasswordAuthorizer {
    public BasicAuthenticator(String username, String password) {
        super(username, password);
    }
}
