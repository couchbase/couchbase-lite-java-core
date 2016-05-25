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
package com.couchbase.lite;

public class CouchbaseLiteException extends Exception {
    private Status status;

    public CouchbaseLiteException(int statusCode) {
        this.status = new Status(statusCode);
    }

    public CouchbaseLiteException(Status status) {
        this.status = status;
    }

    public CouchbaseLiteException(String detailMessage, Status status) {
        super(detailMessage);
        this.status = status;
    }

    public CouchbaseLiteException(String detailMessage, int statusCode) {
        this(detailMessage, new Status(statusCode));
    }

    public CouchbaseLiteException(String detailMessage, Throwable throwable, Status status) {
        super(detailMessage, throwable);
        this.status = status;
    }

    public CouchbaseLiteException(String detailMessage, Throwable throwable, int statusCode) {
        super(detailMessage, throwable);
        this.status = new Status(statusCode);
    }

    public CouchbaseLiteException(Throwable throwable, Status status) {
        super(throwable);
        this.status = status;
    }

    public CouchbaseLiteException(Throwable throwable, int statusCode) {
        super(throwable);
        this.status = new Status(statusCode);
    }

    public Status getCBLStatus() {
        return status;
    }

    @Override
    public String toString() {
        String str = super.toString();
        if (status != null)
            return str + ", " + status.toString();
        else
            return str;
    }
}
