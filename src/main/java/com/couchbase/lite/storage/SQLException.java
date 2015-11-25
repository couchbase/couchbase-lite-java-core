/**
 * Created by Wayne Carter.
 *
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
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

package com.couchbase.lite.storage;

public class SQLException extends RuntimeException {

    // https://www.sqlite.org/rescode.html
    // Note: needs to define other error code
    public static final int SQLITE_OK = 0;
    public static final int SQLITE_ERROR = 1;
    public static final int SQLITE_CONSTRAINT = 19;

    // Extension codes for encryption errors:
    public static final int SQLITE_ENCRYPTION_UNAUTHORIZED = 401;
    public static final int SQLITE_ENCRYPTION_NOTAVAILABLE = 501;
    
    private int code = SQLITE_ERROR;

    public SQLException() {
    }

    public SQLException(String error) {
        super(error);
    }

    public SQLException(int code, String error) {
        super(error);
        this.code = code;
    }

    public SQLException(String error, Throwable cause) {
        super(error, cause);
    }

    public SQLException(Throwable cause) {
        super(cause);
    }

    public SQLException(int code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
