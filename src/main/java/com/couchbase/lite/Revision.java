package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Couchbase Lite Document Revision.
 * <p/>
 * Stores information about a revision -- its docID, revID, and whether it's deleted.
 * <p/>
 * It can also store the sequence number and document contents (they can be added after creation).
 */
public abstract class Revision {

    /**
     * The document this is a revision of
     */
    protected Document document;

    /**
     * The ID of the parentRevision.
     */
    protected String parentRevID;

    /**
     * Constructor
     *
     * @exclude
     */
    @InterfaceAudience.Private
    Revision() {
        super();
    }

    /**
     * Constructor
     *
     * @exclude
     */
    @InterfaceAudience.Private
    protected Revision(Document document) {
        this.document = document;
    }

    /**
     * Get the revision's owning database.
     */
    @InterfaceAudience.Public
    public Database getDatabase() {
        return document.getDatabase();
    }

    /**
     * Get the document this is a revision of.
     */
    @InterfaceAudience.Public
    public Document getDocument() {
        return document;
    }

    /**
     * Gets the Revision's id. In the case of an unsaved revision, may return null.
     */
    @InterfaceAudience.Public
    public abstract String getId();


    /**
     * Does this revision mark the deletion of its document?
     * (In other words, does it have a "_deleted" property?)
     */
    @InterfaceAudience.Public
    public boolean isDeletion() {
        Object deleted = getProperty("_deleted");
        if (deleted == null) {
            return false;
        }
        Boolean deletedBool = (Boolean) deleted;
        return deletedBool.booleanValue();
    }

    /**
     * The contents of this revision of the document.
     * Any keys in the dictionary that begin with "_", such as "_id" and "_rev", contain
     * CouchbaseLite metadata.
     *
     * @return contents of this revision of the document.
     */
    @InterfaceAudience.Public
    public abstract Map<String, Object> getProperties();


    /**
     * The user-defined properties, without the ones reserved by CouchDB.
     * This is based on -properties, with every key whose name starts with "_" removed.
     *
     * @return user-defined properties, without the ones reserved by CouchDB.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getUserProperties() {

        Map<String, Object> result = new HashMap<String, Object>();

        Map<String, Object> sourceMap = getProperties();
        for (String key : sourceMap.keySet()) {
            if (!key.startsWith("_")) {
                result.put(key, sourceMap.get(key));
            }
        }
        return result;
    }

    /**
     * The names of all attachments
     */
    @InterfaceAudience.Public
    public List<String> getAttachmentNames() {
        Map<String, Object> attachmentMetadata = getAttachmentMetadata();
        if (attachmentMetadata == null) {
            return new ArrayList<String>();
        }
        return new ArrayList<String>(attachmentMetadata.keySet());
    }

    /**
     * All attachments, as Attachment objects.
     */
    @InterfaceAudience.Public
    public List<Attachment> getAttachments() {
        Map<String, Object> attachmentMetadata = getAttachmentMetadata();
        if (attachmentMetadata == null) {
            return new ArrayList<Attachment>();
        }
        List<Attachment> result = new ArrayList<Attachment>(attachmentMetadata.size());
        for (Map.Entry<String, Object> entry : attachmentMetadata.entrySet()) {
            Attachment attachment = toAttachment(entry.getKey(), entry.getValue());
            if (attachment != null) {
                result.add(attachment);
            }
        }
        return result;
    }

    /**
     * Shorthand for getProperties().get(key)
     */
    @InterfaceAudience.Public
    public Object getProperty(String key) {
        return getProperties().get(key);
    }

    /**
     * Looks up the attachment with the given name (without fetching its contents yet).
     */
    @InterfaceAudience.Public
    public Attachment getAttachment(String name) {
        Map<String, Object> attachmentsMetadata = getAttachmentMetadata();
        if (attachmentsMetadata == null) {
            return null;
        }
        return toAttachment(name, attachmentsMetadata.get(name));
    }

    private Attachment toAttachment(String name, Object attachment) {
        if (attachment == null) {
            return null;
        } else if (attachment instanceof Attachment) {
            return (Attachment) attachment;
        } else {
            Map<String, Object> metadata = (Map<String, Object>) attachment;
            return new Attachment(this, name, metadata);
        }
    }

    /**
     * Gets the parent Revision.
     */
    @InterfaceAudience.Public
    public abstract SavedRevision getParent();

    /**
     * Gets the parent Revision's id.
     */
    @InterfaceAudience.Public
    public abstract String getParentId();

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    /* package */ abstract long getParentSequence();

    /**
     * Returns the history of this document as an array of CBLRevisions, in forward order.
     * Older revisions are NOT guaranteed to have their properties available.
     *
     * @throws CouchbaseLiteException
     */
    @InterfaceAudience.Public
    public abstract List<SavedRevision> getRevisionHistory() throws CouchbaseLiteException;

    /**
     * Does this revision mark the deletion or removal (from available channels) of its document?
     * (In other words, does it have a "_deleted_ or "_removed" property?)
     */
    @InterfaceAudience.Public
    public boolean isGone() {
        boolean wasRemovedFromChannel = false;
        Object removed = getProperty("_removed");
        if (removed != null) {
            Boolean removedBoolean = (Boolean) removed;
            wasRemovedFromChannel = removedBoolean.booleanValue();
        }
        return isDeletion() || wasRemovedFromChannel;
    }

    /**
     * Compare this revision to the given revision to check for equality.
     * The comparison makes sure that both revisions have the same revision ID.
     *
     * @param o the revision to check for equality against
     * @return true if equal, false otherwise
     */
    @Override
    @InterfaceAudience.Public
    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof SavedRevision) {
            SavedRevision other = (SavedRevision) o;
            if (document.getId().equals(other.getDocument().getId()) && getId().equals(other.getId())) {
                result = true;
            }
        }
        return result;
    }

    /**
     * Custom hashCode based on the hash code of the Document Id and the Revision Id
     */
    @Override
    @InterfaceAudience.Public
    public int hashCode() {
        return document.getId().hashCode() ^ getId().hashCode();
    }

    /**
     * Returns a string representation of this Revision, including the Document Id, the Revision Id
     * and whether or not this Revision is a deletion.
     */
    @Override
    @InterfaceAudience.Public
    public String toString() {
        return "{" + this.document.getId() + " #" + this.getId() + (isDeletion() ? "DEL" : "") + "}";
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected Map<String, Object> getAttachmentMetadata() {
        return (Map<String, Object>) getProperty("_attachments");
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected void setParentRevisionID(String parentRevID) {
        this.parentRevID = parentRevID;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected abstract long getSequence();

    /**
     * Generation number: 1 for a new document, 2 for the 2nd revision, ...
     * Extracted from the numeric prefix of the revID.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    protected int getGeneration() {
        return generationFromRevID(getId());
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static int generationFromRevID(String revID) {
        int generation = 0;
        int length = Math.min(revID == null ? 0 : revID.length(), 9);
        for (int i = 0; i < length; ++i) {
            char c = revID.charAt(i);
            if (Character.isDigit(c))
                generation = 10 * generation + Character.getNumericValue(c);
            else if (c == '-')
                return generation;
            else
                break;
        }
        return 0;
    }
}
