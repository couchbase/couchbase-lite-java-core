package com.couchbase.lite.replicator;

import com.couchbase.lite.Database;
import com.couchbase.lite.internal.Body;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;

import java.util.Map;

/**
 * A revision received from a remote server during a pull. Tracks the opaque remote sequence ID.
 */
@InterfaceAudience.Private
class PulledRevision extends RevisionInternal {

    public PulledRevision(Body body, Database database) {
        super(body, database);
    }

    public PulledRevision(String docId, String revId, boolean deleted, Database database) {
        super(docId, revId, deleted, database);
    }

    public PulledRevision(Map<String, Object> properties, Database database) {
        super(properties, database);
    }

    protected String remoteSequenceID;

    public String getRemoteSequenceID() {
        return remoteSequenceID;
    }

    public void setRemoteSequenceID(String remoteSequenceID) {
        this.remoteSequenceID = remoteSequenceID;
    }

}