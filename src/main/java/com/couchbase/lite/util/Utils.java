package com.couchbase.lite.util;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;

import org.apache.http.StatusLine;
import org.apache.http.client.HttpResponseException;

public class Utils {

    /**
     * Like equals, but works even if either/both are null
     *
     * @param obj1 object1 being compared
     * @param obj2 object2 being compared
     * @return true if both are non-null and obj1.equals(obj2), or true if both are null.
     *         otherwise return false.
     */
    public static boolean isEqual(Object obj1, Object obj2) {
        if (obj1 != null) {
            return (obj2 != null) && obj1.equals(obj2);
        } else {
            return obj2 == null;
        }
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

    public static byte[] byteArrayResultForQuery(SQLiteStorageEngine database, String query, String[] args) throws SQLException {
        byte[] result = null;
        Cursor cursor = null;
        try {
            cursor = database.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getBlob(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /** cribbed from http://stackoverflow.com/questions/9655181/convert-from-byte-array-to-hex-string-in-java */
    final protected static char[] hexArray = "0123456789abcdef".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }


}
