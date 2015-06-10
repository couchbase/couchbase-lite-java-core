package com.couchbase.lite.replicator;

import com.couchbase.lite.internal.Body;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;

import java.util.Map;

/**
 * A revision received from a remote server during a pull. Tracks the opaque remote sequence ID.
 */
@InterfaceAudience.Private
class PulledRevision extends RevisionInternal {

    protected String remoteSequenceID = null;
    protected boolean conflicted = false;

    public PulledRevision(Body body) {
        super(body);
    }

    public PulledRevision(String docId, String revId, boolean deleted) {
        super(docId, revId, deleted);
    }

    public PulledRevision(Map<String, Object> properties) {
        super(properties);
    }

    public String getRemoteSequenceID() {
        return remoteSequenceID;
    }

    public void setRemoteSequenceID(String remoteSequenceID) {
        this.remoteSequenceID = remoteSequenceID;
    }

    public boolean isConflicted() {
        return conflicted;
    }

    public void setConflicted(boolean conflicted) {
        this.conflicted = conflicted;
    }
}