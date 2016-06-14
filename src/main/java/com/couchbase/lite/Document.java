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
import com.couchbase.lite.util.Log;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A CouchbaseLite document.
 */
public class Document {

    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    public static final String TAG = Log.TAG_DATABASE;

    ////////////////////////////////////////////////////////////
    // Inner interfaces & classes
    ////////////////////////////////////////////////////////////

    /**
     * A delegate that can be used to update a Document.
     */
    @InterfaceAudience.Public
    public interface DocumentUpdater {
        /**
         * Document update delegate
         *
         * @param newRevision the unsaved revision about to be saved
         * @return True if the UnsavedRevision should be saved, otherwise false.
         */
        boolean update(UnsavedRevision newRevision);
    }

    /**
     * The type of event raised when a Document changes. This event is not raised in response
     * to local Document changes.
     */
    @InterfaceAudience.Public
    public static class ChangeEvent {
        private Document source;
        private DocumentChange change;

        public ChangeEvent(Document source, DocumentChange documentChange) {
            this.source = source;
            this.change = documentChange;
        }

        public Document getSource() {
            return source;
        }

        public DocumentChange getChange() {
            return change;
        }
    }

    /**
     * A delegate that can be used to listen for Document changes.
     */
    @InterfaceAudience.Public
    public interface ChangeListener {
        void changed(ChangeEvent event);
    }

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    /**
     * The document's owning database.
     */
    private Database database;

    /**
     * The document's ID.
     */
    private String documentId;

    /**
     * The current/latest revision. This object is cached.
     */
    private SavedRevision currentRevision;

    /**
     * Change Listeners
     */
    private final List<ChangeListener> changeListeners = new CopyOnWriteArrayList<ChangeListener>();

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    /**
     * Constructor
     *
     * @param database   The document's owning database
     * @param documentId The document's ID
     * @exclude
     */
    @InterfaceAudience.Private
    public Document(Database database, String documentId) {
        this.database = database;
        this.documentId = documentId;
    }

    ////////////////////////////////////////////////////////////
    // Setter / Getter methods
    ////////////////////////////////////////////////////////////

    /**
     * Get the document's owning database.
     */
    @InterfaceAudience.Public
    public Database getDatabase() {
        return database;
    }

    /**
     * Get the document's ID
     */
    @InterfaceAudience.Public
    public String getId() {
        return documentId;
    }

    /**
     * A date/time after which this document will be automatically purged.
     */
    public Date getExpirationDate() {
        long timestamp = database.getStore().expirationOfDocument(documentId);
        if (timestamp == 0)
            return null;
        return new Date(timestamp);
    }

    public void setExpirationDate(Date date) {
        database.setExpirationDate(date, documentId);
    }

    ////////////////////////////////////////////////////////////
    // Public Methods
    ////////////////////////////////////////////////////////////

    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener changeListener) {
        changeListeners.add(changeListener);
    }

    @InterfaceAudience.Public
    public void removeChangeListener(ChangeListener changeListener) {
        changeListeners.remove(changeListener);
    }

    /**
     * Is this document deleted? (That is, does its current revision have the '_deleted' property?)
     *
     * @return boolean to indicate whether deleted or not
     */
    @InterfaceAudience.Public
    public boolean isDeleted() {
        try {
            return getCurrentRevision() == null && getLeafRevisions().size() > 0;
        } catch (CouchbaseLiteException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the ID of the current revision
     */
    @InterfaceAudience.Public
    public String getCurrentRevisionId() {
        return getCurrentRevision() == null ? null : getCurrentRevision().getId();
    }

    /**
     * Get the current revision
     */
    @InterfaceAudience.Public
    public SavedRevision getCurrentRevision() {
        if (currentRevision == null)
            currentRevision = getRevision(null);
        return currentRevision;
    }

    /**
     * Returns the document's history as an array of CBLRevisions. (See SavedRevision's method.)
     *
     * @return document's history
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Public
    public List<SavedRevision> getRevisionHistory() throws CouchbaseLiteException {
        return getCurrentRevision() == null ? null : getCurrentRevision().getRevisionHistory();
    }

    /**
     * Returns all the current conflicting revisions of the document. If the document is not
     * in conflict, only the single current revision will be returned.
     *
     * @return all current conflicting revisions of the document
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Public
    public List<SavedRevision> getConflictingRevisions() throws CouchbaseLiteException {
        return getLeafRevisions(false);
    }

    /**
     * Returns all the leaf revisions in the document's revision tree,
     * including deleted revisions (i.e. previously-resolved conflicts.)
     *
     * @return all the leaf revisions in the document's revision tree
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Public
    public List<SavedRevision> getLeafRevisions() throws CouchbaseLiteException {
        return getLeafRevisions(true);
    }

    /**
     * The contents of the current revision of the document.
     * This is shorthand for self.currentRevision.properties.
     * Any keys in the dictionary that begin with "_",
     * such as "_id" and "_rev", contain CouchbaseLite metadata.
     *
     * @return contents of the current revision of the document.
     * null if currentRevision is null
     */
    @InterfaceAudience.Public
    public Map<String, Object> getProperties() {
        return getCurrentRevision() == null ? null : getCurrentRevision().getProperties();
    }

    /**
     * The user-defined properties, without the ones reserved by CouchDB.
     * This is based on -properties, with every key whose name starts with "_" removed.
     *
     * @return user-defined properties, without the ones reserved by CouchDB.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getUserProperties() {
        return getCurrentRevision() == null ? null : getCurrentRevision().getUserProperties();
    }

    /**
     * Deletes this document by adding a deletion revision.
     * This will be replicated to other databases.
     *
     * @return boolean to indicate whether deleted or not
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Public
    public boolean delete() throws CouchbaseLiteException {
        return getCurrentRevision() == null ? false : getCurrentRevision().deleteDocument() != null;
    }

    /**
     * Purges this document from the database;
     * this is more than deletion, it forgets entirely about it.
     * The purge will NOT be replicated to other databases.
     *
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Public
    public void purge() throws CouchbaseLiteException {
        Map<String, List<String>> docsToRevs = new HashMap<String, List<String>>();
        List<String> revs = new ArrayList<String>();
        revs.add("*");
        docsToRevs.put(documentId, revs);
        database.purgeRevisions(docsToRevs);
        database.removeDocumentFromCache(this);
    }

    /**
     * The revision with the specified ID.
     *
     * @param revID the revision ID
     * @return the SavedRevision object
     */
    @InterfaceAudience.Public
    public SavedRevision getRevision(String revID) {
        if (revID != null && currentRevision != null && revID.equals(currentRevision.getId()))
            return currentRevision;
        RevisionInternal revisionInternal = database.getDocument(getId(), revID, true);
        return getRevisionFromRev(revisionInternal);
    }

    /**
     * Creates an unsaved new revision whose parent is the currentRevision,
     * or which will be the first revision if the document doesn't exist yet.
     * You can modify this revision's properties and attachments, then save it.
     * No change is made to the database until/unless you save the new revision.
     *
     * @return the newly created revision
     */
    @InterfaceAudience.Public
    public UnsavedRevision createRevision() {
        return new UnsavedRevision(this, getCurrentRevision());
    }

    /**
     * Shorthand for getProperties().get(key)
     */
    @InterfaceAudience.Public
    public Object getProperty(String key) {
        if (getCurrentRevision() != null &&
                getCurrentRevision().getProperties().containsKey(key)) {
            return getCurrentRevision().getProperties().get(key);
        }
        return null;
    }

    /**
     * Saves a new revision. The properties dictionary must have a "_rev" property
     * whose ID matches the current revision's (as it will if it's a modified
     * copy of this document's .properties property.)
     *
     * @param properties the contents to be saved in the new revision
     * @return a new SavedRevision
     */
    @InterfaceAudience.Public
    public SavedRevision putProperties(Map<String, Object> properties)
            throws CouchbaseLiteException {
        String prevID = (String) properties.get("_rev");
        boolean allowConflict = false;
        return putProperties(properties, prevID, allowConflict);
    }

    /**
     * Saves a new revision by letting the caller update the existing properties.
     * This method handles conflicts by retrying (calling the block again).
     * The DocumentUpdater implementation should modify the properties of
     * the new revision and return YES to save or NO to cancel.
     * Be careful: the DocumentUpdater can be called multiple times if there is a conflict!
     *
     * @param updater the callback DocumentUpdater implementation.  Will be called on each
     *                attempt to save. Should update the given revision's properties and then
     *                return YES, or just return NO to cancel.
     * @return The new saved revision, or null on error or cancellation.
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Public
    public SavedRevision update(DocumentUpdater updater) throws CouchbaseLiteException {
        int lastErrorCode = Status.UNKNOWN;
        do {
            // if there is a conflict error, get the latest revision from db instead of cache
            if (lastErrorCode == Status.CONFLICT) {
                forgetCurrentRevision();
            }
            UnsavedRevision newRev = createRevision();
            if (updater.update(newRev) == false) {
                break;
            }
            try {
                SavedRevision savedRev = newRev.save();
                if (savedRev != null) {
                    return savedRev;
                }
            } catch (CouchbaseLiteException e) {
                lastErrorCode = e.getCBLStatus().getCode();
            }

        } while (lastErrorCode == Status.CONFLICT);
        return null;
    }

    /**
     * Adds an existing revision copied from another database. Unlike a normal insertion, this does
     * not assign a new revision ID; instead the revision's ID must be given. The revision's history
     * (ancestry) must be given, which can put it anywhere in the revision tree. It's not an error if
     * the revision already exists locally; it will just be ignored.
     *
     * This is not an operation that clients normally perform; it's used by the replicator.
     * You might want to use it if you're pre-loading a database with canned content, or if you're
     * implementing some new kind of replicator that transfers revisions from another database.
     *
     * @param properties  The properties of the revision (_id and _rev will be ignored, but _deleted
     *                    and _attachments are recognized.)
     * @param attachments  A dictionary providing attachment bodies. The keys are the attachment
     *                     names (matching the keys in the properties' `_attachments` dictionary) and
     *                     the values are the attachment bodies as NSData or NSURL.
     * @param revIDs  The revision history in the form of an array of revision-ID strings, in
     *                reverse chronological order. The first item must be the new revision's ID.
     *                Following items are its parent's ID, etc.
     * @param sourceURL  The URL of the database this revision came from, if any. (This value shows
     *                   up in the CBLDatabaseChange triggered by this insertion, and can help clients
     *                   decide whether the change is local or not.)
     * @return true on success, false on failure.
     * @throws CouchbaseLiteException Error information will be thrown if the insertion fails.
     */
    @InterfaceAudience.Public
    public boolean putExistingRevision(Map<String, Object> properties,
                                       Map<String, Object> attachments,
                                       List<String> revIDs,
                                       URL sourceURL)
            throws CouchbaseLiteException {
        assert (revIDs != null && revIDs.size() > 0);

        boolean deleted = false;
        if (properties != null)
            deleted = properties.get("_deleted") != null &&
                    ((Boolean) properties.get("_deleted")).booleanValue();

        RevisionInternal rev = new RevisionInternal(documentId, revIDs.get(0), deleted);
        rev.setProperties(propertiesToInsert(properties));
        Status status = new Status();
        if (!database.registerAttachmentBodies(attachments, rev, status))
            return false;
        database.forceInsert(rev, revIDs, sourceURL);
        return true;
    }

    ////////////////////////////////////////////////////////////
    // Public Static Methods
    ////////////////////////////////////////////////////////////

    /**
     * in CBLDatabase+Insertion.m
     * + (BOOL) isValidDocumentID: (NSString*)str
     */
    @InterfaceAudience.Private
    public static boolean isValidDocumentId(String id) {
        // http://wiki.apache.org/couchdb/HTTP_Document_API#Documents
        if (id == null || id.length() == 0)
            return false;
        if (id.charAt(0) == '_')
            return (id.startsWith("_design/"));
        return true;
        // "_local/*" is not a valid document ID.
        // Local docs have their own API and shouldn't get here.
    }

    ////////////////////////////////////////////////////////////
    // Protected or Private Methods
    ////////////////////////////////////////////////////////////

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected List<SavedRevision> getLeafRevisions(boolean includeDeleted)
            throws CouchbaseLiteException {
        List<SavedRevision> result = new ArrayList<SavedRevision>();
        RevisionList revs = database.getStore().getAllRevisions(documentId, true);
        if (revs != null) {
            for (RevisionInternal rev : revs) {
                // add it to result, unless we are not supposed to include deleted and it's deleted
                if (!includeDeleted && rev.isDeleted()) {
                    // don't add it
                } else {
                    result.add(getRevisionFromRev(rev));
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    protected Map<String, Object> propertiesToInsert(Map<String, Object> properties)
            throws CouchbaseLiteException {
        String idProp = null;
        if (properties != null && properties.containsKey("_id"))
            idProp = (String) properties.get("_id");

        if (idProp != null && !idProp.equalsIgnoreCase(getId()))
            Log.w(TAG, "Trying to put wrong _id to this: %s properties: %s", this, properties);

        Map<String, Object> nuProperties = new HashMap<String, Object>(properties);

        // Process _attachments dict, converting CBLAttachments to dicts:
        Map<String, Object> attachments = null;
        if (properties != null && properties.containsKey("_attachments"))
            attachments = (Map<String, Object>) properties.get("_attachments");
        if (attachments != null && attachments.size() > 0) {
            Map<String, Object> updatedAttachments =
                    Attachment.installAttachmentBodies(attachments, database);
            nuProperties.put("_attachments", updatedAttachments);
        }

        return nuProperties;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected SavedRevision putProperties(Map<String, Object> properties,
                                          String prevID,
                                          boolean allowConflict)
            throws CouchbaseLiteException {

        boolean hasTrueDeletedProperty = false;
        if (properties != null)
            hasTrueDeletedProperty = properties.get("_deleted") != null &&
                    ((Boolean) properties.get("_deleted")).booleanValue();
        boolean deleted = (properties == null) || hasTrueDeletedProperty;
        RevisionInternal rev = new RevisionInternal(documentId, null, deleted);
        if (properties != null)
            rev.setProperties(propertiesToInsert(properties));
        RevisionInternal newRev = database.putRevision(rev, prevID, allowConflict);
        if (newRev == null)
            return null;
        return new SavedRevision(this, newRev);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected SavedRevision getRevisionFromRev(RevisionInternal internalRevision) {
        if (internalRevision == null) {
            return null;
        } else if (currentRevision != null &&
                internalRevision.getRevID().equals(currentRevision.getId())) {
            return currentRevision;
        } else {
            return new SavedRevision(this, internalRevision);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected void loadCurrentRevisionFrom(QueryRow row) {
        if (row.getDocumentRevisionId() == null) {
            return;
        }
        String revId = row.getDocumentRevisionId();
        if (currentRevision == null || revIdGreaterThanCurrent(revId)) {
            forgetCurrentRevision();
            Map<String, Object> properties = row.getDocumentProperties();
            if (properties != null) {
                RevisionInternal rev = new RevisionInternal(properties);
                currentRevision = new SavedRevision(this, rev);
            }
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private boolean revIdGreaterThanCurrent(String revId) {
        return (RevisionInternal.CBLCompareRevIDs(revId, currentRevision.getId()) > 0);
    }

    /**
     * Notification from the CBLDatabase that a (current, winning) revision has been added
     *
     * @exclude
     */
    @InterfaceAudience.Private
    protected void revisionAdded(DocumentChange change, boolean notify) {
        if (change.getRevisionId() != null) {
            String revID = change.getWinningRevisionID();
            if (revID == null)
                return;  // current revision didn't change

            if (currentRevision != null && !revID.equals(currentRevision.getId())) {
                RevisionInternal rev = change.getWinningRevisionIfKnown();
                if (rev == null)
                    forgetCurrentRevision();
                else if (rev.isDeleted())
                    currentRevision = null;
                else
                    currentRevision = new SavedRevision(this, rev);
            }
        } else {
            // Document was purged!
            currentRevision = null;
        }

        if (notify) {
            for (ChangeListener listener : changeListeners)
                listener.changed(new ChangeEvent(this, change));
        }
    }

    @InterfaceAudience.Private
    protected void forgetCurrentRevision() {
        currentRevision = null;
    }
}
