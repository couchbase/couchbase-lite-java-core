/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 * <p/>
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
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
import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Stores information about a revision -- its docID, revID, and whether it's deleted.
 *
 * It can also store the sequence number and document contents (they can be added after creation).
 */
public final class SavedRevision extends Revision {

    private RevisionInternal revisionInternal;
    private boolean checkedProperties;
    String parentRevID;

    /**
     * Constructor
     * @exclude
     */
    @InterfaceAudience.Private
    protected SavedRevision(Document document, RevisionInternal revision) {
        super(document);
        this.revisionInternal = revision;
    }

    /**
     * Constructor
     * @exclude
     */
    @InterfaceAudience.Private
    protected SavedRevision(Database database, RevisionInternal revision) {
        this(database.getDocument(revision.getDocID()), revision);
    }

    /**
     * Constructor
     * - (instancetype) initForValidationWithDatabase: (CBLDatabase*)db
     *                                       revision: (CBL_Revision*)rev
     *                               parentRevisionID: (NSString*)parentRevID
     */
    @InterfaceAudience.Private
    protected SavedRevision(Database database, RevisionInternal revision, String parentRevID) {
        this(database.getDocument(revision.getDocID()), revision);
        this.parentRevID = parentRevID;
        this.checkedProperties = true;
    }


    /**
     * Get the document this is a revision of
     */
    @InterfaceAudience.Public
    public Document getDocument() {
        return document;
    }

    /**
     * Has this object fetched its contents from the database yet?
     */
    @InterfaceAudience.Public
    public boolean arePropertiesAvailable() {
        return revisionInternal.getProperties() != null;
    }

    @Override
    @InterfaceAudience.Public
    public List<SavedRevision> getRevisionHistory() throws CouchbaseLiteException {
        List<SavedRevision> revisions = new ArrayList<SavedRevision>();
        List<RevisionInternal> internalRevisions = getDatabase().getRevisionHistory(revisionInternal);
        for (RevisionInternal internalRevision : internalRevisions) {
            if (internalRevision.getRevID().equals(getId())) {
                revisions.add(this);
            } else {
                SavedRevision revision = document.getRevisionFromRev(internalRevision);
                revisions.add(revision);
            }

        }
        Collections.reverse(revisions);
        return Collections.unmodifiableList(revisions);
    }


    /**
     * Creates a new mutable child revision whose properties and attachments are initially identical
     * to this one's, which you can modify and then save.
     */
    @InterfaceAudience.Public
    public UnsavedRevision createRevision() {
        return new UnsavedRevision(document, this);
    }

    /**
     * Creates and saves a new revision with the given properties.
     * This will fail with a 412 error if the receiver is not the current revision of the document.
     */
    @InterfaceAudience.Public
    public SavedRevision createRevision(Map<String, Object> properties) throws CouchbaseLiteException {
        boolean allowConflict = false;
        return document.putProperties(properties, revisionInternal.getRevID(), allowConflict);
    }

    @Override
    @InterfaceAudience.Public
    public String getId() {
        return revisionInternal.getRevID();
    }

    @Override
    @InterfaceAudience.Public
    public boolean isDeletion() {
        return revisionInternal.isDeleted();
    }

    /**
     * The contents of this revision of the document.
     * Any keys in the dictionary that begin with "_", such as "_id" and "_rev", contain
     * CouchbaseLite metadata.
     *
     * @return contents of this revision of the document.
     */
    @Override
    @InterfaceAudience.Public
    public Map<String, Object> getProperties() {
        Map<String, Object> properties = revisionInternal.getProperties();
        if (!checkedProperties) {
            if (properties == null) {
                if (loadProperties() == true) {
                    properties = revisionInternal.getProperties();
                }
            }
            checkedProperties = true;
        }
        return properties != null ? Collections.unmodifiableMap(properties) : null;
    }

    /**
     * Deletes the document by creating a new deletion-marker revision.
     *
     * @return
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Public
    public SavedRevision deleteDocument() throws CouchbaseLiteException {
        return createRevision(null);
    }

    @Override
    @InterfaceAudience.Public
    public SavedRevision getParent() {
        return getDocument().getRevisionFromRev(getDatabase().getParentRevision(revisionInternal));
    }

    @Override
    @InterfaceAudience.Public
    public String getParentId() {
        RevisionInternal parRev = getDocument().getDatabase().getParentRevision(revisionInternal);
        if (parRev == null) {
            return null;
        }
        return parRev.getRevID();
    }

    @Override
    @InterfaceAudience.Private
    /* package */ long getParentSequence() {
        SavedRevision parent = getParent();
        return (parent != null) ? parent.getSequence() : 0L;
    }

    @Override
    @InterfaceAudience.Public
    public long getSequence() {
        long sequence = revisionInternal.getSequence();
        if (sequence == 0 && loadProperties()) {
            sequence = revisionInternal.getSequence();
        }
        return sequence;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected boolean loadProperties() {
        try {
            RevisionInternal loadRevision = getDatabase().loadRevisionBody(revisionInternal);
            if (loadRevision == null) {
                Log.w(Database.TAG, "%s: Couldn't load body/sequence", this);
                return false;
            }
            revisionInternal = loadRevision;
            return true;

        } catch (CouchbaseLiteException e) {
            throw new RuntimeException(e);
        }
    }
}
