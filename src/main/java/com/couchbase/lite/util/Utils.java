package com.couchbase.lite.util;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Status;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Utils {

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
        } else if (throwable instanceof HttpResponseException) {
            HttpResponseException e = (HttpResponseException) throwable;
            return isPermanentError(e.getStatusCode());
        } else {
            return false;
        }
    }

    /**
     * in CBLMisc.m
     * BOOL CBLIsPermanentError( NSError* error )
     */
    public static boolean isPermanentError(int code) {
        // TODO: make sure if 406 is acceptable error
        // 406 - in Test cases, server return 406 because of CouchDB API
        //       http://docs.couchdb.org/en/latest/api/database/bulk-api.html
        //       GET /{db}/_all_docs or POST /{db}/_all_docs
        return (code >= 400 && code <= 405) || (code >= 407 && code <= 499);
    }

    public static boolean isTransientError(Throwable throwable) {

        if (throwable instanceof CouchbaseLiteException) {
            CouchbaseLiteException e = (CouchbaseLiteException) throwable;
            return isTransientError(e.getCBLStatus().getCode());
        } else if (throwable instanceof HttpResponseException) {
            HttpResponseException e = (HttpResponseException) throwable;
            return isTransientError(e.getStatusCode());
        } else {
            return false;
        }

    }


    public static boolean isTransientError(StatusLine status) {

        // TODO: in ios implementation, it considers others errors
        /*
            if ($equal(domain, NSURLErrorDomain)) {
        return code == NSURLErrorTimedOut || code == NSURLErrorCannotConnectToHost
                                          || code == NSURLErrorNetworkConnectionLost;
         */

        return isTransientError(status.getStatusCode());

    }

    public static boolean isTransientError(int statusCode) {

        if (statusCode == 500 || statusCode == 502 || statusCode == 503 || statusCode == 504) {
            return true;
        }
        return false;

    }

    /**
     * cribbed from http://stackoverflow.com/questions/9655181/convert-from-byte-array-to-hex-string-in-java
     */
    final protected static char[] hexArray = "0123456789abcdef".toCharArray();

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
        if (e instanceof HttpResponseException) {
            return ((HttpResponseException) e).getStatusCode() == 404;
        }
        return false;
    }

    @InterfaceAudience.Private
    public static int getStatusFromError(Throwable t) {
        if (t instanceof CouchbaseLiteException) {
            CouchbaseLiteException couchbaseLiteException = (CouchbaseLiteException) t;
            return couchbaseLiteException.getCBLStatus().getCode();
        } else if (t instanceof HttpResponseException) {
            HttpResponseException responseException = (HttpResponseException) t;
            return responseException.getStatusCode();
        }
        return Status.UNKNOWN;
    }

    public static String shortenString(String orig, int maxLength) {
        if (orig == null || orig.length() <= maxLength) {
            return orig;
        }
        return orig.substring(0, maxLength);
    }

    // check if contentEncoding is gzip
    public static boolean isGzip(HttpEntity entity) {
        return isGzip(entity.getContentEncoding());
    }

    // check if contentEncoding is gzip
    public static boolean isGzip(Header contentEncoding) {
        return contentEncoding != null && isGzip(contentEncoding.getValue());
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

    public static Map<String, String> headersToMap(Header[] headers) {
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 0; i < headers.length; i++) {
            map.put(headers[i].getName(), headers[i].getValue());
        }
        return map;
    }

    /**
     * The following method shuts down an ExecutorService in two phases,
     * first by calling shutdown to reject incoming tasks, and then calling shutdownNow,
     * if necessary, to cancel any lingering tasks:
     * http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/ExecutorService.html
     *
     * @param timeToWait - Seconds
     */
    public static void shutdownAndAwaitTermination(ExecutorService pool, long timeToWait) {
        synchronized (pool) {
            pool.shutdown(); // Disable new tasks from being submitted
        }
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(timeToWait, TimeUnit.SECONDS)) {
                synchronized (pool) {
                    pool.shutdownNow(); // Cancel currently executing tasks
                }
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(timeToWait, TimeUnit.SECONDS)) {
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
        shutdownAndAwaitTermination(pool, 20);
    }
}