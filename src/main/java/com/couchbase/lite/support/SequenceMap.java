/**
 * Copyright (c) 2016 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.couchbase.lite.support;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * A data structure representing a type of array that allows object values to be added to the end, and removed in arbitrary order;
 * it's used by the replicator to keep track of which revisions have been transferred and what sequences to checkpoint.
 */
public class SequenceMap {
	
	private TreeSet<Long> sequences;  // Sequence numbers currently in the map
	private long lastSequence;        // last generated sequence
	private List<String> values;      // values of remaining sequences
	private long firstValueSequence;  // sequence # of first item in _values
	
	public SequenceMap() {
		sequences = new TreeSet<Long>();
		values = new ArrayList<String>(100);
		firstValueSequence = 1;
		lastSequence = 0;
	}

    /**
     * Adds a value to the map, assigning it a sequence number and returning it.
     * Sequence numbers start at 1 and increment from there.
     */
	public synchronized long addValue(String value) {
		sequences.add(++lastSequence);
		values.add(value);
		return lastSequence;
	}

    /**
     * Removes a sequence and its associated value.
     */
	public synchronized void removeSequence(long sequence) {
		sequences.remove(sequence);
	}
	
	public synchronized boolean isEmpty() {
		return sequences.isEmpty();
	}

    public synchronized int count(){
        return sequences.size();
    }

    /**
     * Returns the maximum consecutively-removed sequence number.
     * This is one less than the minimum remaining sequence number.
     */
	public synchronized long getCheckpointedSequence() {
		long sequence = lastSequence;
		if(!sequences.isEmpty()) {
			sequence = sequences.first() - 1;
		}
		
		if(sequence > firstValueSequence) {
			// Garbage-collect inaccessible values:
			int numToRemove = (int)(sequence - firstValueSequence);
			for(int i = 0; i < numToRemove; i++) {
				values.remove(0);
			}
			firstValueSequence += numToRemove;
		}
		
		return sequence;
	}

    /**
     * Returns the value associated with the checkpointedSequence.
     */
	public synchronized String getCheckpointedValue() {
		int index = (int)(getCheckpointedSequence() - firstValueSequence);
		return (index >= 0) ? values.get(index) : null; 
	}
}
