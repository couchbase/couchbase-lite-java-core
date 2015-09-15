package com.couchbase.lite.storage;

import com.couchbase.lite.CouchbaseLiteException;

/**
 * The interface that specifies shape of SQLiteStorageEngineFactory implementations.
 */
public interface SQLiteStorageEngineFactory {

    SQLiteStorageEngine createStorageEngine(boolean enableEncryption) throws CouchbaseLiteException;

}
