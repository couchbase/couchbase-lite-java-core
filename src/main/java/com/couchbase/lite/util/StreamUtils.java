/**
 * Created by Andrew Reslan.
 * <p/>
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.couchbase.lite.util;

import java.io.*;
import java.util.Iterator;
import java.util.Map;

public class StreamUtils {

    public static void copyStream(InputStream is, OutputStream os) throws IOException {
        int n;
        byte[] buffer = new byte[16384];
        while ((n = is.read(buffer)) > -1) {
            os.write(buffer, 0, n);
        }
        os.close();
        is.close();
    }

    public static void copyStreamsToFolder(Iterator<Map.Entry<String, InputStream>> streams, File folder) throws IOException {

        while (streams.hasNext()) {
            Map.Entry<String, InputStream> entry = streams.next();
            File file = new File(folder, entry.getKey());
            copyStreamToFile(entry.getValue(), file);
        }
    }

    public static void copyStreamToFile(InputStream is, File file) throws IOException {

        OutputStream os = new FileOutputStream(file);
        int n;
        byte[] buffer = new byte[16384];
        while ((n = is.read(buffer)) > -1) {
            os.write(buffer, 0, n);
        }
        os.close();
        is.close();
    }
}
