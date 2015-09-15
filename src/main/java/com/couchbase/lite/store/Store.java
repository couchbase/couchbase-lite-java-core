//
//  Storage.java
//
//  Created by Hideki Itakura on 6/10/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite.store;

import com.couchbase.lite.Attachment;
import com.couchbase.lite.BlobKey;
import com.couchbase.lite.ChangesOptions;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.QueryOptions;
import com.couchbase.lite.ReplicationFilter;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.TransactionalTask;
import com.couchbase.lite.internal.AttachmentInternal;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.SQLiteStorageEngine;

import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstraction of database storage. Every Database has an instance of this,
 * and acts as that instance's delegate.
 */
public interface Store {

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZATION AND CONFIGURATION:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Preflight to see if a database file exists in this directory. Called _before_ open() method!
     */
    boolean databaseExists(String directory);

    /**
     * Opens storage. Files will be created in the directory, which must already exist.
     *
     * @param directory The existing directory to put data files into. The implementation may create
     *                  as many files as it wants here. There will be a subdirectory called
     *                  "attachments" which contains attachments; don't mess with that.
     * @param readOnly  If this is true, the database is opened read-only and any attempt to modify
     *                  it must return an error.
     * @param manager   The owning Manager; this is provided so the storage can examine its
     *                  properties.
     * @return true on success, false on failure.
     */
    //boolean open(String directory, boolean readOnly, Manager manager);
    boolean open() throws CouchbaseLiteException;

    /**
     * Closes storage before it's deallocated.
     */
    void close();

    /**
     * The delegate object, which in practice is the CBLDatabase.
     */
    void setDelegate(StoreDelegate delegate);

    StoreDelegate getDelegate();

    /**
     * The maximum depth a document's revision tree should grow to; beyond that,
     * it should be pruned. This will be set soon after the -openInDirectory call.
     */
    void setMaxRevTreeDepth(int maxRevTreeDepth);

    int getMaxRevTreeDepth();

    /**
     * Whether the database storage should automatically (periodically) be compacted.
     * This will be set soon after the open() method call.
     */
    //void setAutoCompact(boolean value);

    //boolean getAutoCompact();

    ///////////////////////////////////////////////////////////////////////////
    // DATABASE ATTRIBUTES & OPERATIONS:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Stores an arbitrary string under an arbitrary key, persistently.
     */
    long setInfo(String key, String info);

    /**
     * Returns the value assigned to the given key by -setInfo:forKey:.
     */
    String getInfo(String key);

    /**
     * The number of (undeleted) documents in the database.
     */
    int getDocumentCount();

    /**
     * The last sequence number allocated to a revision.
     */
    long getLastSequence();

    /**
     * Is a transaction active?
     */
    boolean inTransaction();

    /**
     * Explicitly compacts document storage.
     */
    void compact() throws CouchbaseLiteException;

    /**
     * Executes the block within a database transaction.
     * If the block returns a non-OK status, the transaction is aborted/rolled back.
     * If the block returns kCBLStatusDBBusy, the block will also be retried after a short delay;
     * if 10 retries all fail, the kCBLStatusDBBusy will be returned to the caller.
     * Any exception raised by the block will be caught and treated as kCBLStatusException.
     */
    boolean runInTransaction(TransactionalTask task);

    ///////////////////////////////////////////////////////////////////////////
    // DOCUMENTS:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Retrieves a document revision by ID.
     *
     * @param docID    The document ID
     * @param revID    The revision ID; may be nil, meaning "the current revision".
     * @param withBody If false, revision's body won't be loaded
     * @return The revision, or nil if not found.
     * @throws CouchbaseLiteException
     */
    RevisionInternal getDocument(String docID, String revID, boolean withBody);
    // throws CouchbaseLiteException;
    //RevisionInternal getDocument(String docID,
    //                             String revID,
    //                             EnumSet<Database.TDContentOptions> contentOptions);

    /**
     * Loads the body of a revision.
     * On entry, rev.docID and rev.revID will be valid.
     * On success, rev.body and rev.sequence will be valid.
     */
    RevisionInternal loadRevisionBody(RevisionInternal rev) throws CouchbaseLiteException;

    /**
     * Looks up the sequence number of a revision.
     * Will only be called on revisions whose .sequence property is not already set.
     * Does not need to set the revision's .sequence property; the caller will take care of that.
     */
    //long getRevisionSequence(RevisionInternal rev);

    /**
     * Retrieves the parent revision of a revision, or returns nil if there is no parent.
     */
    RevisionInternal getParentRevision(RevisionInternal rev);

    /**
     * Returns the given revision's list of direct ancestors (as CBL_Revision objects) in _reverse_
     * chronological order, starting with the revision itself.
     */
    List<RevisionInternal> getRevisionHistory(RevisionInternal rev);

    /**
     * Returns the revision history as a _revisions dictionary, as returned
     * by the REST API's ?revs=true option. If 'ancestorRevIDs' is present,
     * the revision history will only go back as far as any of the revision ID strings
     * in that array.
     */
    //Map<String, Object> getRevisionHistoryDict(RevisionInternal rev);

    /**
     * Returns all the known revisions (or all current/conflicting revisions) of a document.
     *
     * @param docID       The document ID
     * @param onlyCurrent If true, only leaf revisions (whether or not deleted) should be returned.
     * @return An array of all available revisions of the document.
     */
    RevisionList getAllRevisions(String docID, boolean onlyCurrent);

    /**
     * Returns IDs of local revisions of the same document, that have a lower generation number.
     * Does not return revisions whose bodies have been compacted away, or deletion markers.
     * If 'onlyAttachments' is true, only revisions with attachments will be returned.
     */
    List<String> getPossibleAncestorRevisionIDs(RevisionInternal rev,
                                                int limit,
                                                AtomicBoolean onlyAttachments);

    /**
     * Returns the most recent member of revIDs that appears in rev's ancestry.
     * In other words: Look at the revID properties of rev, its parent, grandparent, etc.
     * As soon as you find a revID that's in the revIDs array, stop and return that revID.
     * If no match is found, return nil.
     */
    String findCommonAncestor(RevisionInternal rev, List<String> revIDs);


    /**
     * Looks for each given revision in the local database, and removes each one found
     * from the list. On return, therefore,
     * `revs` will contain only the revisions that don't exist locally.
     */
    //boolean findMissingRevisions(RevisionList touchRevs) throws CouchbaseLiteException;
    int findMissingRevisions(RevisionList touchRevs);

    /**
     * Returns the keys (unique IDs) of all attachments referred to by existing un-compacted
     * Each revision key is an NSData object containing a CBLBlobKey (raw SHA-1 getDigest) derived from
     * the "digest" property of the attachment's metadata.
     */
    Set<BlobKey> findAllAttachmentKeys() throws CouchbaseLiteException;

    /**
     * Iterates over all documents in the database, according to the given query options.
     */
    //List<QueryRow> getAllDocs(QueryOptions options) throws CouchbaseLiteException;
    Map<String, Object> getAllDocs(QueryOptions options) throws CouchbaseLiteException;

    /**
     * Returns all database changes with sequences greater than `lastSequence`.
     *
     * @param lastSequence The sequence number to start _after_
     * @param options      Options for ordering, document content, etc.
     * @param filter       If non-nil, will be called on every revision,
     *                     and those for which it returns NO will be skipped.
     * @return The list of CBL_Revisions
     */
    //RevisionList changesSince(long lastSequence, ChangesOptions options, RevisionFilter filter)
    // throws CouchbaseLiteException;
    RevisionList changesSince(long lastSequence,
                              ChangesOptions options,
                              ReplicationFilter filter,
                              Map<String, Object> filterParams);


    ///////////////////////////////////////////////////////////////////////////
    // INSERTION / DELETION:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Creates a new revision of a document.
     * On success, before returning the new CBL_Revision, the implementation will also call the
     * delegate's -databaseStorageChanged: method to give it more details about the change.
     *
     * @param docID           The document ID, or nil if an ID should be generated at random.
     * @param prevRevID       The parent revision ID, or nil if creating a new document.
     * @param properties      The new revision's properties. (Metadata other than "_attachments" ignored.)
     * @param deleting        YES if this revision is a deletion.
     * @param allowConflict   YES if this operation is allowed to create a conflict; otherwise a 409,
     *                        status will be returned if the parent revision is not a leaf.
     * @param validationBlock If non-nil, this block will be called before the revision is added.
     *                        It's given the parent revision, with its properties if available, and can reject
     *                        the operation by returning an error status.
     * @param outStatus       On return a status will be stored here. Note that on success, the
     *                        status should be 201 for a created revision but 200 for a deletion.
     * @return The new revision, with its revID and sequence filled in, or nil on error.
     */
    RevisionInternal add(String docID,
                         String prevRevID,
                         Map<String, Object> properties,
                         boolean deleting,
                         boolean allowConflict,
                         StorageValidation validationBlock,
                         Status outStatus)
            throws CouchbaseLiteException;

    /**
     * Inserts an already-existing revision (with its revID), plus its ancestry, into a document.
     * This is called by the pull replicator to add the revisions received from the server.
     * On success, the implementation will also call
     * the delegate's -databaseStorageChanged: method to give it more details about the change.
     *
     * @param inRev           The revision to insert. Its revID will be non-nil.
     * @param history         The revIDs of the revision and its ancestors, in reverse chronological order.
     *                        The first item will be equal to inRev.revID.
     * @param validationBlock If non-nil, this block will be called before the revision is added.
     *                        It's given the parent revision, with its properties if available,
     *                        and can reject the operation by returning an error status.
     * @param source          The URL of the remote database this was pulled from, or nil if it's local.
     *                        (This will be used to create the CBLDatabaseChange object sent to the delegate.)
     * @throws CouchbaseLiteException
     */
    void forceInsert(RevisionInternal inRev,
                     List<String> history,
                     StorageValidation validationBlock,
                     URL source)
            throws CouchbaseLiteException;

    /**
     * Purges specific revisions, which deletes them completely from the local database
     * _without_ adding a "tombstone" revision. It's as though they were never there.
     *
     * @param docsToRevs A dictionary mapping document IDs to arrays of revision IDs.
     *                   The magic revision ID "*" means "all revisions", indicating that the
     *                   document should be removed entirely from the database.
     * @return On success will point to an NSDictionary with the same form as docsToRev,
     * containing the doc/revision IDs that were actually removed.
     * @throws CouchbaseLiteException
     */
    Map<String, Object> purgeRevisions(final Map<String, List<String>> docsToRevs);

    ///////////////////////////////////////////////////////////////////////////
    // VIEWS:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Instantiates storage for a view.
     *
     * @param name   The name of the view
     * @param create If YES, the view should be created; otherwise it must already exist
     * @return Storage for the view, or nil if create=NO and it doesn't exist.
     */
    ViewStore getViewStorage(String name, boolean create);

    /**
     * Returns the names of all existing views in the database.
     */
    List<String> getAllViewNames();

    ///////////////////////////////////////////////////////////////////////////
    // LOCAL DOCS:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Returns the contents of a local document. Note that local documents do not have revision
     * histories, so only the current revision exists.
     *
     * @param docID The document ID, which will begin with "_local/"
     * @param revID revID  The revision ID, or nil to return the current revision.
     * @return A revision containing the body of the document, or nil if not found.
     */
    RevisionInternal getLocalDocument(String docID, String revID);

    /**
     * Creates / updates / deletes a local document.
     *
     * @param revision  The new revision to save. Its docID must be set but the revID is ignored.
     *                  If its .deleted property is YES, it's a deletion.
     * @param prevRevID The revision ID to replace
     * @param obeyMVCC  If YES, the prevRevID must match the document's current revID (or nil if the
     *                  document doesn't exist) or a 409 error is returned. If NO, the prevRevID is
     *                  ignored and the operation always succeeds.
     * @return The new revision, with revID filled in, or nil on error.
     * @throws CouchbaseLiteException
     */
    RevisionInternal putLocalRevision(RevisionInternal revision, String prevRevID, boolean obeyMVCC)
            throws CouchbaseLiteException;
}
