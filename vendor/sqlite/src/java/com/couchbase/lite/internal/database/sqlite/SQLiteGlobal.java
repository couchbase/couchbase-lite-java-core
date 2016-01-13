/**
 * Modified by Pasin Suriyentrakorn on 9/21/15.
 * Source: https://github.com/android/platform_frameworks_base/tree/24ff6823c411f794aceaae89b0b029fbf8ef6b29
 *
 * Copyright (c) 2015 Couchbase, Inc.
 *
 * Copyright (C) 2011 The Android Open Source Project
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

import java.lang.reflect.Method;

/**
 * Provides access to SQLite functions that affect all database connection,
 * such as memory management.
 *
 * The native code associated with SQLiteGlobal is also sets global configuration options
 * using sqlite3_config() then calls sqlite3_initialize() to ensure that the SQLite
 * library is properly initialized exactly once before any other framework or application
 * code has a chance to run.
 *
 *
 * @hide
 */
public final class SQLiteGlobal {
    private static final Object sLock = new Object();
    private static int sDefaultPageSize;

    private SQLiteGlobal() {
    }

    /**
     * Gets the default page size to use when creating a database.
     */
    public static int getDefaultPageSize() {
        synchronized (sLock) {
            if (sDefaultPageSize == 0) {
                try {
                    Class clazz = Class.forName("android.os.StatFs");
                    Method m = clazz.getMethod("getBlockSize");
                    Object statFsObj = clazz.getConstructor(String.class).newInstance("/data");
                    Integer value = (Integer) m.invoke(statFsObj, (Object[])null);
                    if (value != null)
                        return value.intValue();
                } catch (Exception e) { }
            }
            if (sDefaultPageSize == 0)
                sDefaultPageSize = 1024;
            return sDefaultPageSize;
        }
    }

    /**
     * The default journal mode to use use when Write-Ahead Logging is not active.
     * Choices are: OFF, DELETE, TRUNCATE, PERSIST and MEMORY.
     * PERSIST may improve performance by reducing how often journal blocks are
     * reallocated (compared to truncation) resulting in better data block locality
     * and less churn of the storage media. -->
     */
    public static String getDefaultJournalMode() {
        return "PERSIST";
    }

    /**
     * Gets the journal size limit in bytes.
     *
     * Maximum size of the persistent journal file in bytes.
     * If the journal file grows to be larger than this amount then SQLite will
     * truncate it after committing the transaction.
     */
    public static int getJournalSizeLimit() {
        return 524288;
    }

    /**
     * Gets the default database synchronization mode when WAL is not in use.
     *
     * The database synchronization mode when using the default journal mode.
     * FULL is safest and preserves durability at the cost of extra fsyncs.
     * NORMAL also preserves durability in non-WAL modes and uses checksums to ensure
     * integrity although there is a small chance that an error might go unnoticed.
     * Choices are: FULL, NORMAL, OFF.
     */
    public static String getDefaultSyncMode() {
        return "FULL";
    }

    /**
     * Gets the database synchronization mode when in WAL mode.
     *
     * The database synchronization mode when using Write-Ahead Logging.
     * FULL is safest and preserves durability at the cost of extra fsyncs.
     * NORMAL sacrifices durability in WAL mode because syncs are only performed before
     * and after checkpoint operations.  If checkpoints are infrequent and power loss
     * occurs, then committed transactions could be lost and applications might break.
     * Choices are: FULL, NORMAL, OFF.
     */
    public static String getWALSyncMode() {
        return "FULL";
    }

    /**
     * Gets the WAL auto-checkpoint integer in database pages.
     *
     * The Write-Ahead Log auto-checkpoint interval in database pages (typically 1 to 4KB).
     * The log is checkpointed automatically whenever it exceeds this many pages.
     * When a database is reopened, its journal mode is set back to the default
     * journal mode, which may cause a checkpoint operation to occur.  Checkpoints
     * can also happen at other times when transactions are committed.
     * The bigger the WAL file, the longer a checkpoint operation takes, so we try
     * to keep the WAL file relatively small to avoid long delays.
     * The size of the WAL file is also constrained by 'db_journal_size_limit'.
     */
    public static int getWALAutoCheckpoint() {
        return 100;
    }

    /**
     * Gets the connection pool size when in WAL mode.
     *
     * Maximum number of database connections opened and managed by framework layer
     * to handle queries on each database when using Write-Ahead Logging.
     */
    public static int getWALConnectionPoolSize() {
        int value = 4;
        return Math.max(2, value);
    }
}
