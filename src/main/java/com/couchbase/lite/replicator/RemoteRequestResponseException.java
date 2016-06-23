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
package com.couchbase.lite.replicator;

import java.io.IOException;
import java.util.Map;

/**
 * Created by hideki on 5/12/16.
 */
public class RemoteRequestResponseException extends IOException {

    // custom error codes
    public static int BAD_URL = -1000; // NSURLErrorBadURL
    public static int USER_DENIED_AUTH = -1012; // NSURLErrorUserCancelledAuthentication

    private final int code;
    private Map userInfo;

    public RemoteRequestResponseException(int code, String s) {
        super(s);
        this.code = code;
        this.userInfo = null;
    }

    public RemoteRequestResponseException(int code, String s, Map userInfo) {
        this(code, s);
        this.userInfo = userInfo;
    }

    public int getCode() {
        return this.code;
    }

    public Map getUserInfo() {
        return userInfo;
    }
}
