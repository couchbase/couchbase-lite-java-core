/**
 * Modified by Pasin Suriyentrakorn on 9/21/15.
 * Source: https://github.com/android/platform_frameworks_base/tree/24ff6823c411f794aceaae89b0b029fbf8ef6b29
 *
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Copyright (C) 2007 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.lite.internal.database.sqlite;

public class SQLiteDebug {
    //TODO: We should make this configuration from SQLiteDabase class:
    public static final boolean DEBUG_LOG_SLOW_QUERIES = false;

    //TODO: We should make this configuration from SQLiteDabase class:
    public static final int DEFAULT_SLOW_QUERY_THRESHOLD = 5000;

    /**
     * Determines whether a query should be logged.
     *
     * Reads the "db.log.slow_query_threshold" system property, which can be changed
     * by the user at any time.  If the value is zero, then all queries will
     * be considered slow.  If the value does not exist or is negative, then no queries will
     * be considered slow.
     *
     * This value can be changed dynamically while the system is running.
     * For example, "adb shell setprop db.log.slow_query_threshold 200" will
     * log all queries that take 200ms or longer to run.
     * @hide
     */
    public static final boolean shouldLogSlowQuery(long elapsedTimeMillis) {
        int slowQueryMillis = DEFAULT_SLOW_QUERY_THRESHOLD;
        return slowQueryMillis >= 0 && elapsedTimeMillis >= slowQueryMillis;
    }

    /**
     * contains statistics about a database
     */
    public static class DbStats {
        /** name of the database */
        public String dbName;

        /** the page size for the database */
        public long pageSize;

        /** the database size */
        public long dbSize;

        /** documented here http://www.sqlite.org/c3ref/c_dbstatus_lookaside_used.html */
        public int lookaside;

        public DbStats(String dbName, long pageCount, long pageSize, int lookaside) {
            this.dbName = dbName;
            this.pageSize = pageSize / 1024;
            dbSize = (pageCount * pageSize) / 1024;
            this.lookaside = lookaside;
        }
    }
}
