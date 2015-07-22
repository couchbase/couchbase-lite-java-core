package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;

import java.net.URL;

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
        return String.format("%s[%s]", this.getClass().getName(), addedRevision);
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
