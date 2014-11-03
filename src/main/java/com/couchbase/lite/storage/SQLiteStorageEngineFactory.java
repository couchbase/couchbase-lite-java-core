package com.couchbase.lite.storage;

/**
 * The interface that specifies shape of SQLiteStorageEngineFactory implementations.
 */
public interface SQLiteStorageEngineFactory {

    public SQLiteStorageEngine createStorageEngine();

}
