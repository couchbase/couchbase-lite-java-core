/**
 * Copyright (c) 2016 Couchbase, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
/**
 * Identifies a change to a database, that is, a newly added document revision.
 * The Database.ChangeEvent contains an List<DocumentChange> in "changes".
 */
public class DocumentChange {
    private RevisionInternal addedRevision;
    private String documentID;
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
        this.documentID = addedRevision.getDocID();
        this.winningRevisionID = winningRevisionID;
        this.isConflict = isConflict;
        this.source = source;
    }

    // - (instancetype) initWithPurgedDocument: (NSString*)docID in CBLDatabaseChange.m
    @InterfaceAudience.Private
    public DocumentChange(String docID) {
        this.documentID = docID;
    }

    /**
     *  The ID of the document that changed.
     */
    @InterfaceAudience.Public
    public String getDocumentId() {
        return documentID;
    }

    /**
     * The ID of the newly-added revision. A nil value means the document was purged.
     */
    @InterfaceAudience.Public
    public String getRevisionId() {
        return addedRevision != null ? addedRevision.getRevID() : null;
    }

    /**
     * Is the new revision the document's current (default, winning) one?
     * If not, there's a conflict.
     */
    @InterfaceAudience.Public
    public boolean isCurrentRevision() {
        return winningRevisionID != null && addedRevision != null &&
                addedRevision.getRevID().equals(winningRevisionID);
    }

    /**
     * YES if the document is in conflict. (The conflict might pre-date this change.)
     */
    @InterfaceAudience.Public
    public boolean isConflict() {
        return isConflict;
    }

    /**
     * The remote database URL that this change was pulled from, if any.
     */
    @InterfaceAudience.Public
    public URL getSource() {
        return source;
    }

    /**
     * YES if the document is deleted
     */
    @InterfaceAudience.Public
    public boolean isDeletion() {
        return addedRevision != null ? addedRevision.isDeleted() : false;
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
        if (addedRevision != null)
            addedRevision = addedRevision.copyWithoutBody();
    }
}
