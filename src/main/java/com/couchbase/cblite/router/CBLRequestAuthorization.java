package com.couchbase.cblite.router;

import com.couchbase.cblite.CBLServer;

public interface CBLRequestAuthorization {
    /**
     * Called by CBLRouter it determines if a request is allowed to proceed or not. If false is
     * returned then it is assumed that the Authorize code has set up the cblurlConnection object
     * with the proper error code, headers, response body (if any), etc. CBLRouter's only
     * responsibility if it detects a 'false' is to send the connection immediately and exit.
     * @param cblServer
     * @param cblurlConnection
     * @return
     */
    public boolean Authorize(CBLServer cblServer, CBLURLConnection cblurlConnection);
}
