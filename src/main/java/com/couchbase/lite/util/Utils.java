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

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Status;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.replicator.RemoteRequestResponseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import okhttp3.Headers;
import okhttp3.Response;

public class Utils {

    public static int DEFAULT_TIME_TO_WAIT_4_SHUTDOWN = 20;
    public static int DEFAULT_TIME_TO_WAIT_4_SHUTDOWNNOW = 20;

    /**
     * Like equals, but works even if either/both are null
     *
     * @param obj1 object1 being compared
     * @param obj2 object2 being compared
     * @return true if both are non-null and obj1.equals(obj2), or true if both are null.
     * otherwise return false.
     */
    public static boolean isEqual(Object obj1, Object obj2) {
        if (obj1 != null) {
            return (obj2 != null) && obj1.equals(obj2);
        } else {
            return obj2 == null;
        }
    }

    /**
     * in CBLMisc.m
     * BOOL CBLIsPermanentError( NSError* error )
     */
    public static boolean isPermanentError(Throwable throwable) {
        if (throwable instanceof CouchbaseLiteException) {
            CouchbaseLiteException e = (CouchbaseLiteException) throwable;
            return isPermanentError(e.getCBLStatus().getCode());
        } else if (throwable instanceof RemoteRequestResponseException) {
            RemoteRequestResponseException e = (RemoteRequestResponseException) throwable;
            return isPermanentError(e.getCode());
        } else {
            return false;
        }
    }

    public static boolean isPermanentError(int code) {

        if (code == RemoteRequestResponseException.BAD_URL
                || code == RemoteRequestResponseException.USER_DENIED_AUTH)
            return true;

        // 406 - in Test cases, server return 406 because of CouchDB API
        //       http://docs.couchdb.org/en/latest/api/database/bulk-api.html
        //       GET /{db}/_all_docs or POST /{db}/_all_docs
        // 408 - Listener might encounter SocketTimeout under weak network connectivity
        return (code >= 400 && code <= 405) || (code == 407) || (code >= 409 && code <= 499);
    }

    /**
     * in CBLMisc.m
     * BOOL CBLMayBeTransientError( NSError* error )
     */
    public static boolean isTransientError(Throwable throwable) {
        if (throwable instanceof CouchbaseLiteException) {
            CouchbaseLiteException e = (CouchbaseLiteException) throwable;
            return isTransientError(e.getCBLStatus().getCode());
        } else if (throwable instanceof RemoteRequestResponseException) {
            RemoteRequestResponseException e = (RemoteRequestResponseException) throwable;
            return isTransientError(e.getCode());
        } else if (throwable instanceof java.net.SocketTimeoutException)
            // connection and socket timeouts => transient error
            return true;
        else if (throwable instanceof java.net.ConnectException)
            return true;
        else if (throwable instanceof java.net.SocketException)
            return true;
        else if (throwable instanceof IOException)
            // NOTE: Exception is thrown from HttpClient.execute() or InputStream.read()
            //       As InputStream.read() thrown IOException, So IOException is considered
            //       temporary issue.
            return true;
        else
            return false;
    }

    public static boolean isTransientError(Response response) {
        return isTransientError(response.code());
    }

    public static boolean isTransientError(int statusCode) {
        if (statusCode == 500 || statusCode == 502 || statusCode == 503 || statusCode == 504 ||
                statusCode == Status.REQUEST_TIMEOUT) {
            return true;
        }
        return false;
    }

    public static boolean isDocumentError(Throwable throwable) {
        if (throwable instanceof CouchbaseLiteException) {
            CouchbaseLiteException e = (CouchbaseLiteException) throwable;
            return isDocumentError(e.getCBLStatus().getCode());
        } else if (throwable instanceof RemoteRequestResponseException) {
            RemoteRequestResponseException e = (RemoteRequestResponseException) throwable;
            return isDocumentError(e.getCode());
        } else {
            return false;
        }
    }

    public static boolean isDocumentError(int statusCode) {
        return (statusCode == Status.NOT_FOUND || statusCode == Status.FORBIDDEN || statusCode == Status.GONE) ? true : false;
    }

    /**
     * cribbed from http://stackoverflow.com/questions/9655181/convert-from-byte-array-to-hex-string-in-java
     */
    protected static final char[] hexArray = "0123456789abcdef".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static void assertNotNull(Object o, String errMsg) {
        if (o == null) {
            throw new IllegalArgumentException(errMsg);
        }
    }

    @InterfaceAudience.Private
    public static boolean is404(Throwable e) {
        if (e instanceof RemoteRequestResponseException) {
            return ((RemoteRequestResponseException) e).getCode() == 404;
        }
        return false;
    }

    @InterfaceAudience.Private
    public static int getStatusFromError(Throwable t) {
        if (t instanceof CouchbaseLiteException) {
            CouchbaseLiteException couchbaseLiteException = (CouchbaseLiteException) t;
            return couchbaseLiteException.getCBLStatus().getCode();
        } else if (t instanceof RemoteRequestResponseException) {
            RemoteRequestResponseException responseException = (RemoteRequestResponseException) t;
            return responseException.getCode();
        }
        return Status.UNKNOWN;
    }

    public static String shortenString(String orig, int maxLength) {
        if (orig == null || orig.length() <= maxLength) {
            return orig;
        }
        return orig.substring(0, maxLength);
    }

    public static boolean isGzip(Response response) {
        return isGzip(response.header("Content-Encoding"));
    }

    // check if contentEncoding is gzip
    public static boolean isGzip(String contentEncoding) {
        return contentEncoding != null && contentEncoding.contains("gzip");
    }

    // to gzip
    public static byte[] compressByGzip(byte[] sourceBytes) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gzip = null;
            try {
                gzip = new GZIPOutputStream(out);
                gzip.write(sourceBytes);
                gzip.close();
            } catch (IOException ex) {
                return null;
            } finally {
                try {
                    if (gzip != null) {
                        gzip.close();
                    }
                } catch (IOException ex) {
                }
            }

            return out.toByteArray();
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
            }
        }
    }

    // from gzip
    public static int CHUNK_SIZE = 8192; // 1024 * 8

    public static byte[] decompressByGzip(byte[] sourceBytes) {
        byte[] buffer = null;
        ByteArrayOutputStream out = null;
        ByteArrayInputStream in = null;
        GZIPInputStream gzip = null;
        try {
            out = new ByteArrayOutputStream();
            buffer = new byte[CHUNK_SIZE];
            in = new ByteArrayInputStream(sourceBytes);
            gzip = new GZIPInputStream(in);
            int len = 0;
            while ((len = gzip.read(buffer, 0, CHUNK_SIZE)) != -1) {
                out.write(buffer, 0, len);
            }
            return out.toByteArray();
        } catch (IOException ex) {
            Log.w(Log.TAG, "Failed to decompress gzipped data: " + ex.getMessage());
            return null;
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
            }
            try {
                if (gzip != null) {
                    gzip.close();
                }
            } catch (IOException ex) {
            }
        }
    }

    public static Map<String, String> headersToMap(Headers headers) {
        Map<String, String> map = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        for (String name : headers.names()) {
            map.put(name, headers.get(name));
        }
        return map;
    }

    /**
     * The following method shuts down an ExecutorService in two phases,
     * first by calling shutdown to reject incoming tasks, and then calling shutdownNow,
     * if necessary, to cancel any lingering tasks:
     * http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/ExecutorService.html
     *
     * @param timeToWait4ShutDown    - Seconds
     * @param timeToWait4ShutDownNow - Seconds
     */
    public static void shutdownAndAwaitTermination(ExecutorService pool,
                                                   long timeToWait4ShutDown,
                                                   long timeToWait4ShutDownNow) {
        synchronized (pool) {
            // Disable new tasks from being submitted
            pool.shutdown();
        }
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(timeToWait4ShutDown, TimeUnit.SECONDS)) {
                synchronized (pool) {
                    pool.shutdownNow(); // Cancel currently executing tasks
                }
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(timeToWait4ShutDownNow, TimeUnit.SECONDS)) {
                    Log.e(Log.TAG_DATABASE, "Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            synchronized (pool) {
                pool.shutdownNow();
            }
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    public static void shutdownAndAwaitTermination(ExecutorService pool) {
        shutdownAndAwaitTermination(pool,
                DEFAULT_TIME_TO_WAIT_4_SHUTDOWN,
                DEFAULT_TIME_TO_WAIT_4_SHUTDOWNNOW);
    }
}