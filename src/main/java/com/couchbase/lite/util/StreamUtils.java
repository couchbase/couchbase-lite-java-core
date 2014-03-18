package com.couchbase.lite.util;

import java.io.*;
import java.util.Map;

/**
 * Created by andy on 18/03/2014.
 */
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

    public static void copyStreamsToFolder(Map<String, InputStream> streams, File folder) throws IOException {

        for (Map.Entry<String, InputStream> entry : streams.entrySet()) {
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
