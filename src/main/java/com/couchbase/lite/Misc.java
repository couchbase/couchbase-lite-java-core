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

import com.couchbase.lite.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @exclude
 */
public class Misc {

    public static String CreateUUID() {
        return UUID.randomUUID().toString().toLowerCase();
    }

    public static String HexSHA1Digest(byte[] input) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Log.e(Database.TAG, "Error, SHA-1 getDigest is unavailable.");
            return null;
        }
        byte[] sha1hash = new byte[40];
        md.update(input, 0, input.length);
        sha1hash = md.digest();
        return convertToHex(sha1hash);
    }

    public static String convertToHex(byte[] data) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            int halfbyte = (data[i] >>> 4) & 0x0F;
            int two_halfs = 0;
            do {
                if ((0 <= halfbyte) && (halfbyte <= 9))
                    buf.append((char) ('0' + halfbyte));
                else
                    buf.append((char) ('a' + (halfbyte - 10)));
                halfbyte = data[i] & 0x0F;
            } while (two_halfs++ < 1);
        }
        return buf.toString();
    }

    public static int SequenceCompare(long a, long b) {
        long diff = a - b;
        return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
    }

    public static String unquoteString(String param) {
        return param.replace("\"", "");
    }

    /**
     * in CBLMisc.m
     * id CBLKeyForPrefixMatch(id key, unsigned depth)
     */
    public static Object keyForPrefixMatch(Object key, int depth) {
        if (depth < 1)
            return key;
        if (key instanceof String) {
            return (key + "\uFFFF");
        } else if (key instanceof List) {
            ArrayList<Map<String, Object>> nuKey = new ArrayList<Map<String, Object>>((List<Map<String, Object>>) key);
            if (depth == 1) {
                nuKey.add(new HashMap<String, Object>());
            } else {
                Object lastObject = nuKey.get(nuKey.size() - 1);
                lastObject = keyForPrefixMatch(lastObject, depth - 1);
                nuKey.set(nuKey.size() - 1, (Map<String, Object>) lastObject);
            }
            return nuKey;
        } else {
            return key;
        }
    }
}

