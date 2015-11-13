/**
 * Created by Pasin Suriyentrakorn on 11/21/15
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

public class SQLiteRevCollator {
    private static native void nativeRegister(long connectionPtr);
    private static native int nativeTestCollate(String string1, String string2);

    public static void register(long connectionPtr) {
        nativeRegister(connectionPtr);
    }

    public static int testCollate(String string1, String string2) {
        return nativeTestCollate(string1, string2);
    }
}
