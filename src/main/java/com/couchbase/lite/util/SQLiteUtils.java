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
package com.couchbase.lite.util;

import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;

public class SQLiteUtils {
    public static byte[] byteArrayResultForQuery(SQLiteStorageEngine storageEngine,
                                                 String query,
                                                 String[] args)
            throws SQLException {
        byte[] result = null;
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getBlob(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    public static long longForQuery(SQLiteStorageEngine storageEngine,
                                    String query,
                                    String[] args)
            throws SQLException {
        Cursor cursor = null;
        long result = 0;
        try {
            cursor = storageEngine.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    public static int intForQuery(SQLiteStorageEngine storageEngine,
                                  String query,
                                  String[] args)
            throws SQLException {
        Cursor cursor = null;
        int result = 0;
        try {
            cursor = storageEngine.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getInt(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    public static boolean booleanForQuery(SQLiteStorageEngine storageEngine,
                                          String query,
                                          String[] args)
            throws SQLException {
        boolean result = false;
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0) == 1L;
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    public static String stringForQuery(SQLiteStorageEngine storageEngine,
                                        String query,
                                        String[] args)
            throws SQLException {
        String result = null;
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    public static void executeUpdate(SQLiteStorageEngine storageEngine,
                                     String query,
                                     String[] args)
            throws SQLException {
        storageEngine.execSQL(query, args);
    }

    public static int changes(SQLiteStorageEngine storageEngine) {
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery("SELECT changes()", null);
            cursor.moveToNext();
            return cursor.getInt(0);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }
}
