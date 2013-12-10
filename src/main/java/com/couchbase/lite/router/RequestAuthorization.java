package com.couchbase.lite.router;

import com.couchbase.lite.Manager;

public interface RequestAuthorization {
    /**
     * Called by Router it determines if a request is allowed to proceed or not. If false is
     * returned then it is assumed that the Authorize code has set up the urlConnection object
     * with the proper error code, headers, response body (if any), etc. Router's only
     * responsibility if it detects a 'false' is to send the connection immediately and exit.
     * @param manager
     * @param urlConnection
     * @return
     */
    public boolean Authorize(Manager manager, URLConnection urlConnection);
}
