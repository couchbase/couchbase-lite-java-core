package com.couchbase.lite.replicator;

import com.github.oxo42.stateless4j.transitions.Transition;

/**
 * Represents a state transition that happens within the replicator
 */
public class ReplicationStateTransition {

    private ReplicationState source;
    private ReplicationState destination;
    private ReplicationTrigger trigger;

    public ReplicationStateTransition(Transition<ReplicationState, ReplicationTrigger> transition) {
        this(transition.getSource(), transition.getDestination(), transition.getTrigger());
    }

    public ReplicationStateTransition(ReplicationState source, ReplicationState destination, ReplicationTrigger trigger) {
        this.source = source;
        this.destination = destination;
        this.trigger = trigger;
    }

    public ReplicationState getSource() {
        return source;
    }

    /* package */ void setSource(ReplicationState source) {
        this.source = source;
    }

    public ReplicationState getDestination() {
        return destination;
    }

    /* package */ void setDestination(ReplicationState destination) {
        this.destination = destination;
    }

    /**
     * @exclude
     */
    public ReplicationTrigger getTrigger() {
        return trigger;
    }

    /* package */ void setTrigger(ReplicationTrigger trigger) {
        this.trigger = trigger;
    }

}
