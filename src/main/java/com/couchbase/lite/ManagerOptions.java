package com.couchbase.lite;

/**
 * Option flags for Manager initialization.
 */
public class ManagerOptions {

    /**
     *  No modifications to databases are allowed.
     */
    private boolean readOnly = false;

    /**
     *  automatically migrate blobstore filename
     */
    private boolean autoMigrateBlobStoreFilename = false;

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
}
