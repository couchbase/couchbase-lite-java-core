package com.couchbase.lite.internal.database.sqlite;

/**
 * Created by pasin on 11/17/15.
 */
public interface SQLiteConnectionListener {
    void onOpen(SQLiteConnection connection);
    void onClose(SQLiteConnection connection);
}
