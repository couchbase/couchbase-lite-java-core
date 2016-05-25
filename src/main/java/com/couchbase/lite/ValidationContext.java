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
package com.couchbase.lite;

import java.util.List;

/**
 * Context passed into a Validator.
 */
public interface ValidationContext {

    /**
     * The contents of the current revision of the document, or nil if this is a new document.
     */
    SavedRevision getCurrentRevision();

    /**
     * Gets the keys whose values have changed between the current and new Revisions
     */
    List<String> getChangedKeys();

    /**
     * Rejects the new Revision.
     */
    void reject();

    /**
     * Rejects the new Revision. The specified message will be included with the resulting error.
     */
    void reject(String message);

    /**
     * Calls the ChangeValidator for each key/value that has changed, passing both the old
     * and new values. If any delegate call returns false, the enumeration stops and false is
     * returned, otherwise true is returned.
     */
    boolean validateChanges(ChangeValidator changeValidator);
}
