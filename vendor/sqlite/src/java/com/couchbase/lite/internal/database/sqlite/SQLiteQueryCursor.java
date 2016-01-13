/**
 * Created by Pasin Suriyentrakorn on 9/21/15
 *
 * Copyright (c) 2015 Couchbase, Inc. All rights reserved.
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

package com.couchbase.lite.internal.database.sqlite;

import com.couchbase.lite.internal.database.cursor.Cursor;
import com.couchbase.lite.internal.database.log.DLog;

public class SQLiteQueryCursor implements Cursor {
    private static final String TAG = DLog.TAG_SQLiteQueryCursor;

    private final SQLiteQuery mQuery;
    private SQLiteConnection.PreparedStatement mPreparedStatement;
    private boolean isAfterLast = false;
    private final String[] mColumns;

    private static native boolean nativeMoveToNext(long statementPtr);
    private static native boolean nativeIsAfterLast(long statementPtr);
    private static native String nativeGetString(long statementPtr, int columnIndex);
    private static native int nativeGetInt(long statementPtr, int columnIndex);
    private static native long nativeGetLong(long statementPtr, int columnIndex);
    private static native double nativeGetDouble(long statementPtr, int columnIndex);
    private static native byte[] nativeGetBlob(long statementPtr, int columnIndex);
    private static native boolean nativeIsNull(long statementPtr, int columnIndex);

    public SQLiteQueryCursor(SQLiteQuery query) {
        this.mQuery = query;

        if (query != null)
            mColumns = query.getColumnNames();
        else
            mColumns = new String[0];
    }

    @Override
    public boolean moveToNext() {
        if (mPreparedStatement == null) {
            mPreparedStatement = mQuery.beginQuery();
        }

        if (mPreparedStatement == null) {
            return false;
        }

        boolean didMove = nativeMoveToNext(mPreparedStatement.mStatementPtr);
        isAfterLast = !didMove;
        return didMove;
    }

    @Override
    public boolean isAfterLast() {
        return isAfterLast;
    }

    private String[] getColumnNames() {
        return mColumns;
    }

    @Override
    public int getColumnIndex(String columnName) {
        final int periodIndex = columnName.lastIndexOf('.');
        if (periodIndex != -1) {
            Exception e = new Exception();
            DLog.e(TAG, "requesting column name with table name -- " + columnName, e);
            columnName = columnName.substring(periodIndex + 1);
        }

        String columnNames[] = getColumnNames();
        int length = columnNames.length;
        for (int i = 0; i < length; i++) {
            if (columnNames[i].equalsIgnoreCase(columnName)) {
                return i;
            }
        }

        return -1;
    }

    @Override
    public int getColumnIndexOrThrow(String columnName) throws IllegalArgumentException {
        final int index = getColumnIndex(columnName);
        if (index < 0) {
            throw new IllegalArgumentException("column '" + columnName + "' does not exist");
        }
        return index;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return getColumnNames()[columnIndex];
    }

    @Override
    public String getString(int columnIndex) {
        if (mPreparedStatement == null) {
            throw new IllegalStateException("No prepared statement.");
        }
        return nativeGetString(mPreparedStatement.mStatementPtr, columnIndex);
    }

    @Override
    public int getShort(int columnIndex) {
        return (short)getLong(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) {
        if (mPreparedStatement == null) {
            throw new IllegalStateException("No prepared statement.");
        }
        return nativeGetInt(mPreparedStatement.mStatementPtr, columnIndex);
    }

    @Override
    public long getLong(int columnIndex) {
        if (mPreparedStatement == null) {
            throw new IllegalStateException("No prepared statement.");
        }
        return nativeGetLong(mPreparedStatement.mStatementPtr, columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) {
        return (float)getDouble(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) {
        if (mPreparedStatement == null) {
            throw new IllegalStateException("No prepared statement.");
        }
        return nativeGetDouble(mPreparedStatement.mStatementPtr, columnIndex);
    }

    @Override
    public byte[] getBlob(int columnIndex) {
        if (mPreparedStatement == null) {
            throw new IllegalStateException("No prepared statement.");
        }
        return nativeGetBlob(mPreparedStatement.mStatementPtr, columnIndex);
    }

    @Override
    public boolean isNull(int columnIndex) {
        if (mPreparedStatement == null) {
            throw new IllegalStateException("No prepared statement.");
        }
        return nativeIsNull(mPreparedStatement.mStatementPtr, columnIndex);
    }

    @Override
    public void close() {
        if (mPreparedStatement == null) {
            return;
        }
        mQuery.endQuery(mPreparedStatement);
    }
}
