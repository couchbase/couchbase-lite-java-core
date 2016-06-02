/**
 * Created by Pasin Suriyentrakorn.
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

import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.NativeLibraryUtils;

public class SQLiteNativeLibrary {
    private static final String TAG = Log.TAG_DATABASE;

    // JNI Native libraries:
    public static final String JNI_SQLITE_CUSTOM_LIBRARY = "cbljavasqlitecustom";
    public static final String JNI_SQLCIPHER_LIBRARY = "cbljavasqlcipher";
    public static final String[] NATIVE_LIBRARY_OPTIONS =
            {JNI_SQLCIPHER_LIBRARY, JNI_SQLITE_CUSTOM_LIBRARY};

    // Use this to override auto loading native libraries:
    public static String TEST_NATIVE_LIBRARY_NAME = null;

    // JNI Key Derivation Native library:
    private static final String JNI_KEY_LIBRARY = "cbljavakey";

    // SQLite library:
    private static final String SHARED_ANDROID_SQLITE_LIBRARY = "sqlite";
    private static final String SHARED_SQLITE_LIBRARY = "sqlite3";

    // SQLCipher library:
    private static final String SHARED_SQLCIPHER_LIBRARY = "sqlcipher";

    public static void load() {
        String[] libraryOption;
        if (TEST_NATIVE_LIBRARY_NAME != null) {
            libraryOption = new String[1];
            libraryOption[0] = TEST_NATIVE_LIBRARY_NAME;
        } else {
            libraryOption = NATIVE_LIBRARY_OPTIONS;
        }

        String loadedLibrary = null;
        boolean success = false;
        for (String libName : libraryOption) {
            if (JNI_SQLCIPHER_LIBRARY.equals(libName)) {
                if (load(SHARED_SQLCIPHER_LIBRARY)) {
                    if (load(JNI_KEY_LIBRARY))
                        success = load(libName);
                }
            } else if (JNI_SQLITE_CUSTOM_LIBRARY.equals(libName)) {
                if (load(SHARED_SQLITE_LIBRARY))
                    success = load(libName);
            } else
                Log.e(TAG, "Unknown native library name : " + libName);

            if (success) {
                loadedLibrary = libName;
                break;
            }
        }

        if (success)
            Log.v(TAG, "Successfully load native library: " + loadedLibrary);
        else
            Log.e(TAG, "Cannot load native library");
    }

    private static boolean load(String libName) {
        try {
            if (isAndriod()) {
                return loadSystemLibrary(libName);
            } else {
                return NativeLibraryUtils.loadLibrary(libName);
            }
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    private static boolean loadSystemLibrary(String libName) {
        try {
            System.loadLibrary(libName);
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
        return true;
    }

    private static boolean isAndriod() {
        return (System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik"));
    }
}
