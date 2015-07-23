/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 * <p/>
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.couchbase.lite;

/**
 * Same interpretation as HTTP status codes, esp. 200, 201, 404, 409, 500.
 */
public class Status {

    public static final int UNKNOWN = -1;

    public static final int OK = 200;
    public static final int CREATED = 201;
    public static final int ACCEPTED = 202;

    public static final int NOT_MODIFIED = 304;

    public static final int BAD_REQUEST = 400;
    public static final int UNAUTHORIZED = 401;
    public static final int FORBIDDEN = 403;
    public static final int NOT_FOUND = 404;
    public static final int METHOD_NOT_ALLOWED = 405;
    public static final int NOT_ACCEPTABLE = 406;
    public static final int CONFLICT = 409;
    public static final int DUPLICATE = 412;// Formally known as "Precondition Failed" (PRECONDITION_FAILED)
    //public static final int PRECONDITION_FAILED = DUPLICATE;
    public static final int UNSUPPORTED_TYPE = 415;

    public static final int INTERNAL_SERVER_ERROR = 500;
    public static final int NOT_IMPLEMENTED = 501;

    // Non-HTTP errors:
    public static final int BAD_ENCODING = 490;
    public static final int BAD_ATTACHMENT = 491;
    public static final int ATTACHMENT_NOT_FOUND = 492;
    public static final int BAD_JSON = 493;
    public static final int BAD_ID = 494;
    public static final int BAD_PARAM = 495;
    public static final int DELETED = 496; // Document deleted

    public static final int BAD_CHANGES_FEED = 587;
    public static final int CHANGES_FEED_TRUNCATED = 588;
    public static final int UPSTREAM_ERROR = 589;// Error from remote replication server
    public static final int DB_ERROR = 590;// SQLite error
    public static final int CORRUPT_ERROR = 591;// bad data in database
    public static final int ATTACHMENT_ERROR = 592;// problem with attachment store
    public static final int CALLBACK_ERROR = 593;// app callback (emit fn, etc.) failed
    public static final int EXCEPTION = 594; // Exception raised/caught
    public static final int DB_BUSY = 595; // SQLite DB is busy (this is recoverable!)

    private int code;

    public Status() {
        this.code = UNKNOWN;
    }

    public Status(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public boolean isSuccessful() {
        return (code > 0 && code < 400);
    }

    public boolean isError() {
        return !isSuccessful();
    }

    @Override
    public String toString() {
        return "Status: " + code;
    }

}
