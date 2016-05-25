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
}