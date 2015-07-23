//
//  RevisionFilter.java
//
//  Created by Hideki Itakura on 6/10/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite.store;

import com.couchbase.lite.internal.RevisionInternal;

/**
 * A block that can filter revisions by passing or rejecting them.
 */
public interface RevisionFilter {
    boolean filter(RevisionInternal rev);
}
