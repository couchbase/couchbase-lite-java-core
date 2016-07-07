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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicationStateTransition that = (ReplicationStateTransition) o;

        if (source != that.source) return false;
        if (destination != that.destination) return false;
        return trigger == that.trigger;

    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
        result = 31 * result + destination.hashCode();
        result = 31 * result + trigger.hashCode();
        return result;
    }
}
