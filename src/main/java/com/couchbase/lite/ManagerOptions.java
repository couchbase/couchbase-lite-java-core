package com.couchbase.lite;

/**
 * Option flags for Manager initialization.
 */
public class ManagerOptions {

    /**
     * No modifications to databases are allowed.
     */
    private boolean readOnly = false;

    /**
     * automatically migrate blobstore filename
     */
    private boolean autoMigrateBlobStoreFilename = false;

    /**
     * the number of threads to keep in the pool for RemoteRequestExecutor
     * ReplicationInternal.java: public static final int EXECUTOR_THREAD_POOL_SIZE = 5;
     * https://github.com/couchbase/couchbase-lite-java-core/issues/343
     */
    private int executorThreadPoolSize = 0;

    private String storeClassName = null;

    public ManagerOptions() {
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isAutoMigrateBlobStoreFilename() {
        return autoMigrateBlobStoreFilename;
    }

    public void setAutoMigrateBlobStoreFilename(boolean autoMigrateBlobStoreFilename) {
        this.autoMigrateBlobStoreFilename = autoMigrateBlobStoreFilename;
    }

    public int getExecutorThreadPoolSize() {
        return executorThreadPoolSize;
    }

    public void setExecutorThreadPoolSize(int executorThreadPoolSize) {
        this.executorThreadPoolSize = executorThreadPoolSize;
    }

    public String getStoreClassName() {
        return storeClassName;
    }

    public void setStoreClassName(String storeClassName) {
        this.storeClassName = storeClassName;
    }
}