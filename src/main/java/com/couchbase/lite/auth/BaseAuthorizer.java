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
 * Created by hideki on 6/14/16.
 */
public abstract class BaseAuthorizer implements Authorizer {
    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////
    protected URL remoteURL;

    ////////////////////////////////////////////////////////////
    // Implementation of Authenticator
    ////////////////////////////////////////////////////////////
    @Override
    public String getUsername() {
        // @optional
        return null;
    }

    @Override
    public boolean implementedLoginResponse() {
        return false;
    }

    ////////////////////////////////////////////////////////////
    // Implementations of IAuthorizer
    ////////////////////////////////////////////////////////////

    @Override
    public URL getRemoteURL() {
        return remoteURL;
    }

    @Override
    public void setRemoteURL(URL remoteURL) {
        this.remoteURL = remoteURL;
    }
}
