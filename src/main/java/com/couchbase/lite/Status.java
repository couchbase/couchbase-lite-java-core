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

import java.util.HashMap;
import java.util.Map;

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
    public static final int REQUEST_TIMEOUT = 408;
    public static final int CONFLICT = 409;
    public static final int GONE = 410;
    public static final int DUPLICATE = 412;                // Formally known as "Precondition Failed" (PRECONDITION_FAILED)
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
    public static final int DELETED = 496;                  // Document deleted
    public static final int INVALID_STORAGE_TYPE = 497;
    public static final int BAD_CHANGES_FEED = 587;
    public static final int CHANGES_FEED_TRUNCATED = 588;
    public static final int UPSTREAM_ERROR = 589;           // Error from remote replication server
    public static final int DB_ERROR = 590;                 // SQLite error
    public static final int CORRUPT_ERROR = 591;            // bad data in database
    public static final int ATTACHMENT_ERROR = 592;         // problem with attachment store
    public static final int CALLBACK_ERROR = 593;           // app callback (emit fn, etc.) failed
    public static final int EXCEPTION = 594;                // Exception raised/caught
    public static final int DB_BUSY = 595;                  // SQLite DB is busy (this is recoverable!)

    public static class HTTPStatus {
        private int code;
        private String message;

        public HTTPStatus(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return "HTTP " + code + ' ' + message;
        }
    }

    private static final Map<Integer, HTTPStatus> statusMap;
    static {
        statusMap = new HashMap<Integer, HTTPStatus>();

        // General HTTP Status Code:
        statusMap.put(OK,                       new HTTPStatus(200, "OK"));
        statusMap.put(CREATED,                  new HTTPStatus(201, "Created"));
        statusMap.put(ACCEPTED,                 new HTTPStatus(202, "Accepted"));
        statusMap.put(NOT_MODIFIED,             new HTTPStatus(304, "Not Modified"));
        statusMap.put(INTERNAL_SERVER_ERROR,    new HTTPStatus(500, "Internal Server Error"));
        statusMap.put(NOT_IMPLEMENTED,          new HTTPStatus(501, "Not Implemented"));

        // For compatibility with CouchDB, return the same strings it does (see couch_httpd.erl)
        statusMap.put(BAD_REQUEST,              new HTTPStatus(400, "bad_request"));
        statusMap.put(UNAUTHORIZED,             new HTTPStatus(401, "unauthorized"));
        statusMap.put(FORBIDDEN,                new HTTPStatus(403, "forbidden"));
        statusMap.put(NOT_FOUND,                new HTTPStatus(404, "not_found"));
        statusMap.put(METHOD_NOT_ALLOWED,       new HTTPStatus(405, "method_not_allowed"));
        statusMap.put(NOT_ACCEPTABLE,           new HTTPStatus(406, "not_acceptable"));
        statusMap.put(REQUEST_TIMEOUT,          new HTTPStatus(408, "request_timeout"));
        statusMap.put(CONFLICT,                 new HTTPStatus(409, "conflict"));
        statusMap.put(GONE,                     new HTTPStatus(410, "gone"));
        statusMap.put(DUPLICATE,                new HTTPStatus(412, "file_exists"));
        statusMap.put(UNSUPPORTED_TYPE,         new HTTPStatus(415, "bad_content_type"));

        // These are nonstandard status codes; map them to closest HTTP equivalents:
        statusMap.put(UNKNOWN,                  new HTTPStatus(500, "Internal error"));
        statusMap.put(BAD_ENCODING,             new HTTPStatus(400, "Bad data encoding"));
        statusMap.put(BAD_ATTACHMENT,           new HTTPStatus(400, "Invalid attachment"));
        statusMap.put(ATTACHMENT_NOT_FOUND,     new HTTPStatus(404, "Attachment not found"));
        statusMap.put(BAD_JSON,                 new HTTPStatus(400, "Invalid JSON"));
        statusMap.put(BAD_ID,                   new HTTPStatus(400, "Invalid database/document/revision ID"));
        statusMap.put(BAD_PARAM,                new HTTPStatus(400, "Invalid parameter in HTTP query or JSON body"));
        statusMap.put(DELETED,                  new HTTPStatus(404, "Deleted"));
        statusMap.put(INVALID_STORAGE_TYPE,     new HTTPStatus(406, "Can't open database in that storage format"));
        statusMap.put(BAD_CHANGES_FEED,         new HTTPStatus(502, "Server changes feed parse error"));
        statusMap.put(CHANGES_FEED_TRUNCATED,   new HTTPStatus(502, "Server changes feed truncated"));
        statusMap.put(UPSTREAM_ERROR,           new HTTPStatus(502, "Invalid response from remote replication server"));
        statusMap.put(DB_ERROR,                 new HTTPStatus(500, "Database error!"));
        statusMap.put(CORRUPT_ERROR,            new HTTPStatus(500, "Invalid data in database"));
        statusMap.put(ATTACHMENT_ERROR,         new HTTPStatus(500, "Attachment store error"));
        statusMap.put(CALLBACK_ERROR,           new HTTPStatus(500, "Application callback block failed"));
        statusMap.put(EXCEPTION,                new HTTPStatus(500, "Internal error"));
        statusMap.put(DB_BUSY,                  new HTTPStatus(500, "Database locked"));
    }

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
        return "Status: " + code + " (" + getHTTPStatus().toString() + ')';
    }

    public HTTPStatus getHTTPStatus() {
        HTTPStatus httpStatus = statusMap.get(getCode());
        if (httpStatus == null)
            httpStatus = statusMap.get(UNKNOWN);
        return httpStatus;
    }

    public int getHTTPCode() {
        return getHTTPStatus().getCode();
    }

    public String getHTTPMessage() {
        return getHTTPStatus().getMessage();
    }
}
