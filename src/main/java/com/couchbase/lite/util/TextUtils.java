/*
 * Copyright (C) 2006 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.lite.util;

import com.couchbase.lite.internal.InterfaceAudience;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

// COPY: Partially copied from android.text.TextUtils
public class TextUtils {
    /**
     * Returns a string containing the tokens joined by delimiters.
     *
     * @param tokens an array objects to be joined. Strings will be formed from
     *               the objects by calling object.toString().
     */
    public static String join(CharSequence delimiter, Iterable tokens) {
        StringBuilder sb = new StringBuilder();
        boolean firstTime = true;
        for (Object token : tokens) {
            if (firstTime) {
                firstTime = false;
            } else {
                sb.append(delimiter);
            }
            sb.append(token);
        }
        return sb.toString();
    }

    public static byte[] read(InputStream is) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            byte[] bytes = new byte[512];
            int offset = 0;
            int numRead;
            while ((numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
                buffer.write(bytes, 0, numRead);
                offset += numRead;
            }
            return buffer.toByteArray();
        } finally {
            buffer.close();
        }
    }

    public static String read(File file) throws IOException {
        byte[] content = null;
        InputStream input = new FileInputStream(file);
        try {
            content = read(input);
        } finally {
            input.close();
        }
        return content != null ? new String(content) : null;
    }

    public static void write(String text, File file) throws IOException {
        BufferedOutputStream output = null;
        try {
            output = new BufferedOutputStream(new FileOutputStream(file));
            output.write(text.getBytes());
        } finally {
            if (output != null)
                output.close();
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static String joinQuoted(List<String> strings) {
        if (strings.size() == 0) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        result.append("'");
        boolean first = true;
        for (String string : strings) {
            if (first) {
                first = false;
            } else {
                result.append("','");
            }
            result.append(quote(string));
        }
        result.append('\'');

        return result.toString();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static String joinQuotedObjects(List<Object> objects) {
        List<String> strings = new ArrayList<String>();
        for (Object object : objects) {
            strings.add(object != null ? object.toString() : null);
        }
        return joinQuoted(strings);
    }

    public static String quote(String string) {
        return string.replace("'", "''");
    }
}
