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
package com.couchbase.lite.store;

import com.couchbase.lite.DocumentChange;
import com.couchbase.lite.ReplicationFilter;
import com.couchbase.lite.internal.RevisionInternal;

import java.util.Map;

/**
 * Delegate of a CBL_Storage instance. CBLDatabase implements this.
 */
public interface StoreDelegate {
    /**
     * Called whenever the outermost transaction completes.
     *
     * @param committed YES on commit, NO if the transaction was aborted.
     */
    void storageExitedTransaction(boolean committed);

    /**
     * Called whenever a revision is added to the database (but not for local docs or for purges.)
     */
    void databaseStorageChanged(DocumentChange change);

    /**
     * Generates a revision ID for a new revision.
     *
     * @param json      The canonical JSON of the revision (with metadata properties removed.)
     * @param deleted   YES if this revision is a deletion
     * @param prevRevID The parent's revision ID, or nil if this is a new document.
     */
    String generateRevID(byte[] json, boolean deleted, String prevRevID);

    // TODO: This minght not be a delegate methods. Review later.
    boolean runFilter(ReplicationFilter filter,
                      Map<String, Object> filterParams,
                      RevisionInternal rev);
}
