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
 * Internal protocol for authenticating a user to a server.
 * (The word "authorization" here is a misnomer, but HTTP uses it for historical reasons.)
 */
public interface Authorizer extends Authenticator {
    /**
     * The base URL of the remote service. The replicator sets this property when it starts up.
     */
    URL getRemoteURL();

    void setRemoteURL(URL remoteURL);

    /**
     * The unique ID of the local database. The replicator sets this property when it starts up.
     */
    String getLocalUUID();

    void setLocalUUID(String localUUID);

    boolean removeStoredCredentials();

    // @optional
    String getUsername();
}
