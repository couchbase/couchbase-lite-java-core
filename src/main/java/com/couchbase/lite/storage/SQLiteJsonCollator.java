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

import java.text.Collator;

public class SQLiteJsonCollator {
    private static native void nativeRegister(long connectionPtr, String locale, String icuDataPath);
    private static native int nativeTestCollate(int rule, int len1, String string1, int len2, String string2);
    private static native int nativeTestCollateWithLocale(int rule, String locale, int len1, String string1, int len2, String string2);
    private static native int nativeTestDigitToInt(int digit);
    private static native char nativeTestEscape(String source);

    public static int compareStringsUnicode(String a, String b) {
        Collator c = Collator.getInstance();
        int res = c.compare(a, b);
        return res;
    }

    public static void register(long connectionPtr, String locale, String icuDataPath) {
        nativeRegister(connectionPtr, locale, icuDataPath);
    }

    public static int testCollate(int rule, String string1, String string2) {
        return nativeTestCollate(rule, string1.length(), string1, string2.length(), string2);
    }

    public static int testCollate(int rule, String locale, String string1, String string2) {
        return nativeTestCollateWithLocale(rule, locale, string1.length(), string1, string2.length(), string2);
    }

    public static int testDigitToInt(int digit) {
        return nativeTestDigitToInt(digit);
    }

    public static char testEscape (String source) {
        return nativeTestEscape(source);
    }
}
