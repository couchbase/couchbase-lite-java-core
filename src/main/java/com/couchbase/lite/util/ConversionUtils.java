//
// Copyright (c) 2016 Couchbase, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
//
package com.couchbase.lite.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * Utility class for converting
 */
public class ConversionUtils {
    public static byte[] toByteArray(Map obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                try {
                    oos.writeObject(obj);
                    return bos.toByteArray();
                } finally {
                    oos.close();
                }
            } finally {
                bos.close();
            }
        } catch (IOException ioe) {
            Log.e(Log.TAG, "Error in toByteArray()", ioe);
        }
        return null;
    }

    public static Map fromByteArray(byte[] bytes) throws IOException {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            try {
                ObjectInputStream ois = new ObjectInputStream(bis);
                try {
                    return (Map) ois.readObject();
                } finally {
                    ois.close();
                }
            } finally {
                bis.close();
            }
        } catch (IOException ioe) {
            Log.e(Log.TAG, "Error in fromByteArray()", ioe);
        } catch (ClassNotFoundException cnfe) {
            Log.e(Log.TAG, "Error in fromByteArray()", cnfe);
        }
        return null;
    }
}
