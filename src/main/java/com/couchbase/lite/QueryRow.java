package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.store.QueryRowStore;
import com.couchbase.lite.util.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A result row from a CouchbaseLite view query.
 * Full-text and geo queries return subclasses -- see CBLFullTextQueryRow and CBLGeoQueryRow.
 */
public class QueryRow {

    /**
     * The row's key: this is the first parameter passed to the emit() call that generated the row.
     */
    private Object key;

    /**
     * The row's value: this is the second parameter passed to the emit() call that generated the row.
     */
    private Object value;

    /**
     * The database sequence number of the associated doc/revision.
     */
    private long sequence;

    /**
     * The ID of the document that caused this view row to be emitted.
     * This is the value of the "id" property of the JSON view row.
     * It will be the same as the .documentID property, unless the map function caused a
     * related document to be linked by adding an "_id" key to the emitted value; in this
     * case .documentID will refer to the linked document, while sourceDocumentID always
     * refers to the original document.
     * In a reduced or grouped query the value will be nil, since the rows don't correspond
     * to individual documents.
     */
    private String sourceDocID;

    /**
     * The properties of the document this row was mapped from.
     * To get this, you must have set the .prefetch property on the query; else this will be nil.
     * (You can still get the document properties via the .document property, of course. But it
     * takes a separate call to the database. So if you're doing it for every row, using
     * .prefetch and .documentProperties is faster.)
     */
    //private Map<String, Object> documentProperties;
    RevisionInternal documentRevision;

    private QueryRowStore queryRowStore;

    private Database database;

    /**
     * Constructor
     * <p/>
     * The database property will be filled in when I'm added to a QueryEnumerator.
     */
    @InterfaceAudience.Private
    public QueryRow(String docID,
                    long sequence,
                    Object key,
                    Object value,
                    RevisionInternal docRevision,
                    QueryRowStore queryRowStore) {
        // Don't initialize _database yet. I might be instantiated on a background thread (if the
        // query is async) which has a different CBLDatabase instance than the original caller.
        // Instead, the database property will be filled in when I'm added to a CBLQueryEnumerator.
        this.sourceDocID = docID;
        this.sequence = sequence;
        this.key = key;
        this.value = value;
        this.documentRevision = docRevision;
        this.queryRowStore = queryRowStore;
    }


    protected Database getDatabase() {
        return database;
    }

    protected void setDatabase(Database database) {
        this.database = database;
    }


    /**
     * The document this row was mapped from.  This will be nil if a grouping was enabled in
     * the query, because then the result rows don't correspond to individual documents.
     */
    @InterfaceAudience.Public
    public Document getDocument() {
        if (getDocumentId() == null) {
            return null;
        }
        assert (database != null);
        Document document = database.getDocument(getDocumentId());
        document.loadCurrentRevisionFrom(this);
        return document;
    }

    /**
     * The row's key: this is the first parameter passed to the emit() call that generated the row.
     */
    @InterfaceAudience.Public
    public Object getKey() {
        return key;
    }

    /**
     * The row's value: this is the second parameter passed to the emit() call that generated the row.
     */
    @InterfaceAudience.Public
    public Object getValue() {
        return value;
    }

    /**
     * The ID of the document described by this view row.  This is not necessarily the same as the
     * document that caused this row to be emitted; see the discussion of the .sourceDocumentID
     * property for details.
     */
    @InterfaceAudience.Public
    public String getDocumentId() {
        // Get the doc id from either the embedded document contents, or the '_id' value key.
        // Failing that, there's no document linking, so use the regular old _sourceDocID
        String docID = null;
        if (documentRevision != null)
            docID = documentRevision.getDocID();
        if (docID == null) {
            if (value != null) {
                Map<String, Object> props = (Map<String, Object>) value;
                docID = (String) props.get("_id");
            }
        }
        if (docID == null)
            docID = sourceDocID;
        return docID;
    }

    /**
     * The ID of the document that caused this view row to be emitted.  This is the value of
     * the "id" property of the JSON view row. It will be the same as the .documentID property,
     * unless the map function caused a related document to be linked by adding an "_id" key to
     * the emitted value; in this case .documentID will refer to the linked document, while
     * sourceDocumentID always refers to the original document.  In a reduced or grouped query
     * the value will be nil, since the rows don't correspond to individual documents.
     */
    @InterfaceAudience.Public
    public String getSourceDocumentId() {
        return sourceDocID;
    }

    /**
     * The revision ID of the document this row was mapped from.
     */
    @InterfaceAudience.Public
    public String getDocumentRevisionId() {
        // Get the revision id from either the embedded document contents,
        // or the '_rev' or 'rev' value key:
        String rev = null;
        if (documentRevision != null)
            rev = documentRevision.getRevID();
        if (rev == null) {
            if (value instanceof Map) {
                Map<String, Object> mapValue = (Map<String, Object>) value;
                rev = (String) mapValue.get("_rev");
                if (rev == null) {
                    rev = (String) mapValue.get("rev");
                }
            }
        }
        return rev;
    }

    /**
     * The properties of the document this row was mapped from.
     * To get this, you must have set the -prefetch property on the query; else this will be nil.
     * The map returned is immutable (run through Collections.unmodifiableMap)
     */
    @InterfaceAudience.Public
    public Map<String, Object> getDocumentProperties() {
        return documentRevision != null ? documentRevision.getProperties() : null;
    }

    /**
     * The local sequence number of the associated doc/revision.
     */
    @InterfaceAudience.Public
    public long getSequenceNumber() {
        return sequence;
    }

    /**
     * Returns all conflicting revisions of the document, or nil if the
     * document is not in conflict.
     * <p/>
     * The first object in the array will be the default "winning" revision that shadows the others.
     * This is only valid in an allDocuments query whose allDocsMode is set to Query.AllDocsMode.SHOW_CONFLICTS
     * or Query.AllDocsMode.ONLY_CONFLICTS; otherwise it returns an empty list.
     */
    @InterfaceAudience.Public
    public List<SavedRevision> getConflictingRevisions() {
        Document doc = database.getDocument(sourceDocID);
        Map<String, Object> valueTmp = (Map<String, Object>) value;
        List<String> conflicts = (List<String>) valueTmp.get("_conflicts");
        if (conflicts == null) {
            conflicts = new ArrayList<String>();
        }
        List<SavedRevision> conflictingRevisions = new ArrayList<SavedRevision>();
        for (String conflictRevisionId : conflicts) {
            SavedRevision revision = doc.getRevision(conflictRevisionId);
            conflictingRevisions.add(revision);
        }
        return conflictingRevisions;
    }

    /**
     * Compare this against the given QueryRow for equality.
     * <p/>
     * Implentation Note: This is used implicitly by -[LiveQuery update] to decide whether
     * the query result has changed enough to notify the client. So it's important that it
     * not give false positives, else the app won't get notified of changes.
     *
     * @param object the QueryRow to compare this instance with.
     * @return true if equal, false otherwise.
     */
    @Override
    @InterfaceAudience.Public
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof QueryRow)) {
            return false;
        }
        QueryRow other = (QueryRow) object;

        if (database == other.database
                && Utils.isEqual(key, other.getKey())
                && Utils.isEqual(sourceDocID, other.getSourceDocumentId())
                && Utils.isEqual(documentRevision, other.documentRevision)) {
            // If values were emitted, compare them. Otherwise we have nothing to go on so check
            // if _anything_ about the doc has changed (i.e. the sequences are different.)
            if (value != null || other.getValue() != null) {
                return value.equals(other.getValue());
            } else {
                return sequence == other.sequence;
            }
        }
        return false;
    }

    /**
     * Return a string representation of this QueryRow.
     * The string is returned in JSON format.
     *
     * @return the JSON string representing this QueryRow
     */
    @Override
    @InterfaceAudience.Public
    public String toString() {
        return asJSONDictionary().toString();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Map<String, Object> asJSONDictionary() {
        Map<String, Object> result = new HashMap<String, Object>();
        if (value != null || sourceDocID != null) {
            result.put("key", key);
            result.put("value", value);
            result.put("id", sourceDocID);
            result.put("doc", getDocumentProperties());
        } else {
            result.put("key", key);
            result.put("error", "not_found");
        }
        return result;
    }
}
