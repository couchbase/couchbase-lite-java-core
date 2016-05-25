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

import com.couchbase.lite.storage.SQLiteStorageEngineFactory;

import java.io.File;

/**
 * The Couchbase Lite context is an abstract wrapper around platform specific context.  Eg,
 * there are platform specific implementations such as AndroidContext and JavaContext.
 *
 * The wrapper is needed so that there are no compile time dependencies on Android classes
 * within the Couchbase Lite java core library.
 *
 * This also has the nice side effect of having a single place to see exactly what parts of the
 * Android context are being used.
 */
public interface Context {

    /**
     * The files dir.  On Android implementation, simply proxies call to underlying Context
     */
    File getFilesDir();

    /**
     * Get temporary directory. The temporary directory will be used to store temporary files.
     */
    File getTempDir();

    /**
     * Override the default behavior and set your own NetworkReachabilityManager subclass,
     * which allows you to completely control how to respond to network reachability changes
     * in your app affects the replicators that are listening for change events.
     */
    void setNetworkReachabilityManager(NetworkReachabilityManager networkReachabilityManager);

    /**
     * Replicators call this to get the NetworkReachabilityManager, and they register/unregister
     * themselves to receive network reachability callbacks.
     *
     * If setNetworkReachabilityManager() was called prior to this, that instance will be used.
     * Otherwise, the context will create a new default reachability manager and return that.
     */
    NetworkReachabilityManager getNetworkReachabilityManager();


    /**
     * Get the SQLiteStorageEngineFactory, or null if none has been set, in which case
     * the default will be used.
     */
    SQLiteStorageEngineFactory getSQLiteStorageEngineFactory();

    /**
     * Return User-Agent value
     */
    String getUserAgent();
}
