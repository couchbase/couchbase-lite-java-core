package com.couchbase.lite.replicator;

/**
 * The various states that a Replication can be in
 */
public enum ReplicationState {
    INITIAL,
    RUNNING,
    IDLE,
    OFFLINE,
    STOPPING,
    STOPPED
}
