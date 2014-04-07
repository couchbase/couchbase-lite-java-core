package com.couchbase.lite.util;

import org.apache.http.StatusLine;

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

    public static boolean isTransientError(StatusLine status) {

        // TODO: in ios implementation, it considers others errors
        /*
            if ($equal(domain, NSURLErrorDomain)) {
        return code == NSURLErrorTimedOut || code == NSURLErrorCannotConnectToHost
                                          || code == NSURLErrorNetworkConnectionLost;
         */

        int code = status.getStatusCode();
        if (code == 500 || code == 502 || code == 503 || code == 504) {
            return true;
        }
        return false;

    }

}
