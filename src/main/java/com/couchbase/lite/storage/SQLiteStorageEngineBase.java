/**
 * Created by Pasin Suriyentrakorn on 11/24/15
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

package com.couchbase.lite.storage;

import com.couchbase.lite.internal.database.ContentValues;
import com.couchbase.lite.internal.database.DatabasePlatformSupport;
import com.couchbase.lite.internal.database.security.Key;
import com.couchbase.lite.internal.database.sqlite.SQLiteConnection;
import com.couchbase.lite.internal.database.sqlite.SQLiteConnectionListener;
import com.couchbase.lite.internal.database.sqlite.SQLiteDatabase;
import com.couchbase.lite.internal.database.sqlite.exception.SQLiteConstraintException;
import com.couchbase.lite.internal.database.sqlite.exception.SQLiteDatabaseCorruptException;
import com.couchbase.lite.support.security.SymmetricKey;
import com.couchbase.lite.util.Log;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SQLiteStorageEngineBase implements SQLiteStorageEngine {
    static {
        SQLiteNativeLibrary.load();
    }

    private static final String TAG = Log.TAG_DATABASE;

    private SQLiteDatabase database;
    private AtomicBoolean isSupportEncryption = null;
    private SymmetricKey encryptionKey;

    abstract protected DatabasePlatformSupport getDatabasePlatformSupport();

    abstract protected String getICUDatabasePath();

    /**
     * Gets the connection pool size when in WAL mode.
     *
     * Maximum number of database connections opened and managed by framework layer
     * to handle queries on each database when using Write-Ahead Logging.
     */
    abstract protected int getWALConnectionPoolSize();

    @Override
    public boolean open(String path, SymmetricKey encryptionKey) throws SQLException {
        if(database != null && database.isOpen())
            return true;

        boolean hasError = false;
        try {
            if (encryptionKey != null && !supportEncryption()) {
                Log.w(TAG, "Encryption not available (app not built with SQLCipher)");
                throw new SQLException(SQLException.SQLITE_ENCRYPTION_NOTAVAILABLE,
                        "Encryption not available");
            }
            this.encryptionKey = encryptionKey;

            SQLiteDatabase.setDatabasePlatformSupport(getDatabasePlatformSupport());
            database = SQLiteDatabase.openDatabase(path, null,
                    SQLiteDatabase.CREATE_IF_NECESSARY | SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING,
                    getWALConnectionPoolSize(),
                    null, new ConnectionListener());
            Log.v(Log.TAG_DATABASE, "%s: Opened Android sqlite db", this);
        } catch(SQLiteDatabaseCorruptException e) {
            hasError = true;
            Log.e(Log.TAG_DATABASE, "Unauthorize to open the SQLite database", e);
            throw new SQLException(SQLException.SQLITE_ENCRYPTION_UNAUTHORIZED,
                    "Cannot decrypt or access the database");
        } catch(com.couchbase.lite.internal.database.SQLException e) {
            hasError = true;
            Log.e(Log.TAG_DATABASE, "Unable to open the SQLite database", e);
            throw new SQLException(e);
        } finally {
            if (hasError && database != null)
                database.close();
        }

        return database.isOpen();
    }

    private class ConnectionListener implements SQLiteConnectionListener {
        @Override
        public void onOpen(SQLiteConnection connection) {
            // Decrypt database if encrypted:
            decrypt(connection, encryptionKey);

            // Register JSON Collator:
            String icuDataPath = getICUDatabasePath();
            SQLiteJsonCollator.register(connection.getConnectionHandle(),
                    connection.getLocale().toString(), icuDataPath);
            // Register Rev Collator:
            SQLiteRevCollator.register(connection.getConnectionHandle());
        }

        @Override
        public void onClose(SQLiteConnection connection) { }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Database Encryption
    ///////////////////////////////////////////////////////////////////////////

    private static void decrypt(SQLiteConnection connection, SymmetricKey key)
            throws com.couchbase.lite.internal.database.SQLException {
        if (key != null) {
            try {
                connection.execute("PRAGMA key = \"x'" + key.getHexData() + "'\"", null, null);
            } catch (SQLException e) {
                Log.w(TAG, "'pragma key' failed", e);
                throw e;
            }
        }

        try {
            connection.executeForLong("SELECT count(*) FROM sqlite_master", null, null);
        } catch (com.couchbase.lite.internal.database.SQLException e) {
            Log.w(TAG, "Decrypting database failed", e);
            throw e;
        }
    }

    @Override
    public int getVersion() {
        return database.getVersion();
    }

    @Override
    public void setVersion(int version) {
        database.setVersion(version);
    }

    @Override
    public boolean isOpen() {
        return database != null && database.isOpen();
    }

    @Override
    public void beginTransaction() {
        database.beginTransaction();
    }

    @Override
    public void endTransaction() {
        database.endTransaction();
    }

    @Override
    public void setTransactionSuccessful() {
        database.setTransactionSuccessful();
    }

    @Override
    public void execSQL(String sql) throws SQLException {
        try {
            database.execSQL(sql);
        } catch (com.couchbase.lite.internal.database.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void execSQL(String sql, Object[] bindArgs) throws SQLException {
        try {
            database.execSQL(sql, bindArgs);
        } catch (com.couchbase.lite.internal.database.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Cursor rawQuery(String sql, String[] selectionArgs) {
        return new SQLiteCursor(database.rawQuery(sql, selectionArgs));
    }

    @Override
    public long insert(String table, String nullColumnHack, ContentValues values) {
        return database.insert(table, nullColumnHack, values);
    }

    @Override
    public long insertOrThrow(String table, String nullColumnHack, ContentValues values)
            throws SQLException {
        try {
            return database.insertOrThrow(table, nullColumnHack, values);
        } catch (com.couchbase.lite.internal.database.SQLException e) {
            if(e instanceof SQLiteConstraintException)
                throw new SQLException(SQLException.SQLITE_CONSTRAINT, e);
            else
                throw new SQLException(e);
        }
    }

    @Override
    public long insertWithOnConflict(String table, String nullColumnHack,
                                     ContentValues initialValues, int conflictAlgorithm) {
        return database.insertWithOnConflict(table, nullColumnHack, initialValues, conflictAlgorithm);
    }

    @Override
    public int update(String table, ContentValues values, String whereClause, String[] whereArgs) {
        return database.update(table, values, whereClause, whereArgs);
    }

    @Override
    public int delete(String table, String whereClause, String[] whereArgs) {
        return database.delete(table, whereClause, whereArgs);
    }

    @Override
    public void close() {
        database.close();
    }

    @Override
    public boolean supportEncryption() {
        if (isSupportEncryption == null) {
            boolean support = SQLiteDatabase.isSupportEncryption();
            isSupportEncryption = new AtomicBoolean(support);
        }
        return isSupportEncryption.get();
    }

    @Override
    public byte[] derivePBKDF2SHA256Key(String password, byte[] salt, int rounds) {
        if (!supportEncryption())
            return null;
        return Key.derivePBKDF2SHA256Key(password, salt, rounds);
    }

    @Override
    public String toString() {
        return "SQLiteStorageEngine {" +
                "database=" + Integer.toHexString(System.identityHashCode(database)) +
                '}';
    }

    private class SQLiteCursor implements Cursor {
        private com.couchbase.lite.internal.database.cursor.Cursor cursor;

        public SQLiteCursor(com.couchbase.lite.internal.database.cursor.Cursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean moveToNext() {
            return cursor.moveToNext();
        }

        @Override
        public boolean isAfterLast() {
            return cursor.isAfterLast();
        }

        @Override
        public String getString(int columnIndex) {
            return cursor.getString(columnIndex);
        }

        @Override
        public int getInt(int columnIndex) {
            return cursor.getInt(columnIndex);
        }

        @Override
        public long getLong(int columnIndex) {
            return cursor.getLong(columnIndex);
        }

        @Override
        public byte[] getBlob(int columnIndex) {
            return cursor.getBlob(columnIndex);
        }

        @Override
        public void close() {
            cursor.close();
        }

        @Override
        public boolean isNull(int columnIndex) {
            return cursor.isNull(columnIndex);
        }
    }
}
