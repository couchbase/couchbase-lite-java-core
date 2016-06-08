/**
 * Created by Pasin Suriyentrakorn on 9/26/15
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
package com.couchbase.lite.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class NativeLibraryUtils {
    private static final Map<String, Boolean> LOADED_LIBRARIES = new HashMap<String, Boolean>();

    public static boolean loadLibrary(String libraryName) {
        if (LOADED_LIBRARIES.containsKey(libraryName))
            return true;

        try {
            File libraryFile;
            String libraryPath = getConfiguredLibraryPath(libraryName);
            if (libraryPath != null)
                libraryFile = new File(libraryPath);
            else
                libraryFile = extractLibrary(libraryName);

            if (libraryFile != null) {
                System.load(libraryFile.getAbsolutePath());
                LOADED_LIBRARIES.put(libraryName, true);
            } else {
                Log.e(Log.TAG, "Cannot find library: " + libraryName);
                return false;
            }
        } catch (Exception e) {
            Log.e(Log.TAG, "Error loading library: " + libraryName, e);
            return false;
        }

        return true;
    }

    private static String getConfiguredLibraryPath(String libraryName) {
        String key = String.format(Locale.ENGLISH, "com.couchbase.lite.lib.%s.path", libraryName);
        return System.getProperty(key);
    }

    private static String getLibraryFullName(String libraryName) {
        String name = System.mapLibraryName(libraryName);
        // Workaround discrepancy issue between OSX Java6 (.jnilib)
        // and Java7 (.dylib) native library file extension.
        if (name.endsWith(".jnilib")) {
            name = name.replace(".jnilib", ".dylib");
        }
        return name;
    }

    private static File extractLibrary(String libraryName) throws IOException {
        String libraryResourcePath = getLibraryResourcePath(libraryName);
        String targetFolder = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();
        File targetFile = new File(targetFolder, getLibraryFullName(libraryName));

        // If the target already exists, and it's unchanged, then use it, otherwise delete it and
        // it will be replaced.
        if (targetFile.exists()) {
            // Remove old native library file:
            if (!targetFile.delete()) {
                // If we can't remove the old library file then log a warning and try to use it:
                Log.w(Log.TAG, "Failed to delete existing library file: " + targetFile.getAbsolutePath());
                return targetFile;
            }
        }

        // Extract the library to the target directory:
        InputStream libraryReader = NativeLibraryUtils.class.getResourceAsStream(libraryResourcePath);
        if (libraryReader == null) {
            System.err.println("Library not found: " + libraryResourcePath);
            return null;
        }

        FileOutputStream libraryWriter = new FileOutputStream(targetFile);
        try {
            byte[] buffer = new byte[1024];
            int bytesRead = 0;
            while ((bytesRead = libraryReader.read(buffer)) != -1) {
                libraryWriter.write(buffer, 0, bytesRead);
            }
        } finally {
            libraryWriter.close();
            libraryReader.close();
        }

        // On non-windows systems set up permissions for the extracted native library.
        if (!System.getProperty("os.name").toLowerCase().contains("windows")) {
            try {
                Runtime.getRuntime().exec(
                        new String[]{"chmod", "755", targetFile.getAbsolutePath()}).waitFor();
            } catch (Throwable e) {
                Log.e(Log.TAG, "Error executing 'chmod 755' on extracted native library", e);
            }
        }
        return targetFile;
    }

    private static String getLibraryResourcePath(String libraryName) {
        // Root native folder.
        String path = "/native";

        // OS part of path.
        String osName = System.getProperty("os.name");
        if (osName.contains("Linux")) {
            path += "/linux";
        } else if (osName.contains("Mac")) {
            path += "/osx";
        } else if (osName.contains("Windows")) {
            path += "/windows";
        } else {
            path += '/' + osName.replaceAll("\\W", "").toLowerCase();
        }

        // Architecture part of path.
        String archName = System.getProperty("os.arch");
        archName = archName.replaceAll("\\W", "");
        archName = archName.replace("-", "_");
        path += '/' + archName;

        // Platform specific name part of path.
        path += '/' + getLibraryFullName(libraryName);
        return path;
    }
}
