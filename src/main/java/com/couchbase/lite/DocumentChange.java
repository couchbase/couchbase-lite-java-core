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

import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;

import java.net.URL;
import java.util.Locale;

/**
 * Provides details about a Document change.
 */
public class DocumentChange {
    private RevisionInternal addedRevision;
    private String winningRevisionID;
    private boolean isConflict;
    private URL source;

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public DocumentChange(RevisionInternal addedRevision,
                          String winningRevisionID,
                          boolean isConflict,
                          URL source) {
        this.addedRevision = addedRevision;
        this.winningRevisionID = winningRevisionID;
        this.isConflict = isConflict;
        this.source = source;
    }

    @InterfaceAudience.Public
    public String getDocumentId() {
        return addedRevision.getDocID();
    }

    @InterfaceAudience.Public
    public String getRevisionId() {
        return addedRevision.getRevID();
    }

    @InterfaceAudience.Public
    public boolean isCurrentRevision() {
        return winningRevisionID != null && addedRevision.getRevID().equals(winningRevisionID);
    }

    @InterfaceAudience.Public
    public boolean isConflict() {
        return isConflict;
    }

    @InterfaceAudience.Public
    public URL getSource() {
        return source;
    }

    @InterfaceAudience.Public
    public String toString() {
        return String.format(Locale.ENGLISH, "%s[%s]", this.getClass().getName(), addedRevision);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public RevisionInternal getAddedRevision() {
        return addedRevision;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected RevisionInternal getWinningRevisionIfKnown() {
        return isCurrentRevision() ? addedRevision : null;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public String getWinningRevisionID() {
        return winningRevisionID;
    }

    protected void reduceMemoryUsage() {
        addedRevision = addedRevision.copyWithoutBody();
    }
}
