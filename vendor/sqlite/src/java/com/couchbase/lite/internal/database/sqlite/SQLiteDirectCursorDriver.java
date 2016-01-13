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

import com.couchbase.lite.internal.database.cursor.Cursor;
import com.couchbase.lite.internal.database.CancellationSignal;

/**
 * A cursor driver that uses the given query directly.
 * 
 * @hide
 */
public final class SQLiteDirectCursorDriver implements SQLiteCursorDriver {
    private final SQLiteDatabase mDatabase;
    private final String mEditTable; 
    private final String mSql;
    private final CancellationSignal mCancellationSignal;
    private SQLiteQuery mQuery;

    public SQLiteDirectCursorDriver(SQLiteDatabase db, String sql, String editTable,
            CancellationSignal cancellationSignal) {
        mDatabase = db;
        mEditTable = editTable;
        mSql = sql;
        mCancellationSignal = cancellationSignal;
    }

    public Cursor query(SQLiteDatabase.CursorFactory factory, String[] selectionArgs) {
        final SQLiteQuery query = new SQLiteQuery(mDatabase, mSql, mCancellationSignal);
        final Cursor cursor;
        try {
            query.bindAllArgsAsStrings(selectionArgs);

            if (factory == null) {
                cursor = new com.couchbase.lite.internal.database.sqlite.SQLiteQueryCursor(query);
            } else {
                cursor = factory.newCursor(mDatabase, this, mEditTable, query);
            }
        } catch (RuntimeException ex) {
            query.close();
            throw ex;
        }

        mQuery = query;
        return cursor;
    }

    public void cursorClosed() {
        // Do nothing
    }

    public void setBindArguments(String[] bindArgs) {
        mQuery.bindAllArgsAsStrings(bindArgs);
    }

    public void cursorDeactivated() {
        // Do nothing
    }

    public void cursorRequeried(Cursor cursor) {
        // Do nothing
    }

    @Override
    public String toString() {
        return "SQLiteDirectCursorDriver: " + mSql;
    }
}
