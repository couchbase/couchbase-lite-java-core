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

package com.couchbase.lite.internal;

import com.couchbase.lite.util.CollectionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Stores information about a revision -- its docID, revID, and whether it's deleted.
 * <p/>
 * It can also store the sequence number and document contents (they can be added after creation).
 */
public class RevisionInternal {

    private String docID;
    private String revID;
    private boolean deleted;
    private boolean missing;
    private Body body;
    private long sequence;

    public RevisionInternal(String docID, String revID, boolean deleted) {
        this.docID = docID;
        this.revID = revID;
        this.deleted = deleted;
    }

    public RevisionInternal(Body body) {
        this((String) body.getPropertyForKey("_id"),
                (String) body.getPropertyForKey("_rev"),
                (((Boolean) body.getPropertyForKey("_deleted") != null)
                        && ((Boolean) body.getPropertyForKey("_deleted") == true)));
        this.body = body;
    }

    public RevisionInternal(Map<String, Object> properties) {
        this(new Body(properties));
    }

    public Map<String, Object> getProperties() {
        //return body == null ? null : body.getProperties();
        Map<String, Object> result = null;
        if (body != null) {
            Map<String, Object> prop;
            try {
                prop = body.getProperties();
            } catch (IllegalStateException e) {
                // handle when both object and json are null for this body
                return null;
            }
            if (result == null) {
                result = new HashMap<String, Object>();
            }
            result.putAll(prop);
        }
        return result;
    }

    public Object getPropertyForKey(String key) {
        Map<String, Object> prop = getProperties();
        if (prop == null) {
            return null;
        }
        return prop.get(key);
    }

    public void setProperties(Map<String, Object> properties) {
        this.body = new Body(properties);
    }

    public byte[] getJson() {
        byte[] result = null;
        if (body != null) {
            result = body.getJson();
        }
        return result;
    }

    public void setJSON(byte[] json) {
        this.body = new Body(json, docID, revID, deleted);
    }

    @Override
    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof RevisionInternal) {
            RevisionInternal other = (RevisionInternal) o;
            if (docID.equals(other.docID) && revID.equals(other.revID)) {
                result = true;
            }
        }
        return result;
    }

    @Override
    public int hashCode() {
        return docID.hashCode() ^ revID.hashCode();
    }

    public String getDocID() {
        return docID;
    }

    public void setDocID(String docID) {
        this.docID = docID;
    }

    public String getRevID() {
        return revID;
    }

    public void setRevID(String revID) {
        this.revID = revID;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public Body getBody() {
        return body;
    }

    public void setBody(Body body) {
        this.body = body;
    }

    public boolean isMissing() {
        return missing;
    }

    public void setMissing(boolean missing) {
        this.missing = missing;
    }

    public RevisionInternal copy() {
        return copyWithDocID(docID, revID);
    }

    public RevisionInternal copyWithDocID(String docID, String revID) {
        assert (docID != null);
        assert ((this.docID == null) || (this.docID.equals(docID)));
        RevisionInternal result = new RevisionInternal(docID, revID, deleted);
        Map<String, Object> unmodifiableProperties = getProperties();
        Map<String, Object> properties = new HashMap<String, Object>();
        if (unmodifiableProperties != null) {
            properties.putAll(unmodifiableProperties);
        }
        properties.put("_id", docID);
        properties.put("_rev", revID);
        result.setProperties(properties);
        return result;
    }

    public RevisionInternal copyWithoutBody() {
        if (body == null) {
            return this;
        }
        RevisionInternal rev = new RevisionInternal(docID, revID, deleted);
        rev.setSequence(sequence);
        rev.setMissing(missing);
        return rev;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "{" + this.docID + " #" + this.revID + (deleted ? "DEL" : "") + "}";
    }

    /**
     * Generation number: 1 for a new document, 2 for the 2nd revision, ...
     * Extracted from the numeric prefix of the revID.
     */
    public int getGeneration() {
        return generationFromRevID(revID);
    }

    public static int generationFromRevID(String revID) {
        if(revID == null) return 0;
        int generation = 0;
        int dashPos = revID.indexOf("-");
        if (dashPos > 0) {
            generation = Integer.parseInt(revID.substring(0, dashPos));
        }
        return generation;
    }

    public static String digestFromRevID(String revID) {
        String digest = "error";
        int dashPos = revID.indexOf("-");
        if (dashPos > 0) {
            digest = revID.substring(dashPos + 1);
            return digest;
        }
        throw new RuntimeException(String.format("Invalid rev id: %s", revID));
    }

    public static int CBLCollateRevIDs(String revId1, String revId2) {

        String rev1GenerationStr = null;
        String rev2GenerationStr = null;
        String rev1Hash = null;
        String rev2Hash = null;

        StringTokenizer st1 = new StringTokenizer(revId1, "-");
        try {
            rev1GenerationStr = st1.nextToken();
            rev1Hash = st1.nextToken();
        } catch (Exception e) {
        }

        StringTokenizer st2 = new StringTokenizer(revId2, "-");
        try {
            rev2GenerationStr = st2.nextToken();
            rev2Hash = st2.nextToken();
        } catch (Exception e) {
        }

        // improper rev IDs; just compare as plain text:
        if (rev1GenerationStr == null || rev2GenerationStr == null) {
            return revId1.compareToIgnoreCase(revId2);
        }

        Integer rev1Generation;
        Integer rev2Generation;

        try {
            rev1Generation = Integer.parseInt(rev1GenerationStr);
            rev2Generation = Integer.parseInt(rev2GenerationStr);
        } catch (NumberFormatException e) {
            // improper rev IDs; just compare as plain text:
            return revId1.compareToIgnoreCase(revId2);
        }

        // Compare generation numbers; if they match, compare suffixes:
        if (rev1Generation.compareTo(rev2Generation) != 0) {
            return rev1Generation.compareTo(rev2Generation);
        } else if (rev1Hash != null && rev2Hash != null) {
            // compare suffixes if possible
            return rev1Hash.compareTo(rev2Hash);
        } else {
            // just compare as plain text:
            return revId1.compareToIgnoreCase(revId2);
        }

    }

    public static int CBLCompareRevIDs(String revId1, String revId2) {
        assert (revId1 != null);
        assert (revId2 != null);
        return CBLCollateRevIDs(revId1, revId2);
    }

    // Calls the block on every attachment dictionary. The block can return a different dictionary,
    // which will be replaced in the rev's properties. If it returns nil, the operation aborts.
    // Returns YES if any changes were made.
    public boolean mutateAttachments(CollectionUtils.Functor<Map<String, Object>,
            Map<String, Object>> functor) {
        {
            Map<String, Object> properties = getProperties();
            Map<String, Object> editedProperties = null;
            Map<String, Object> attachments = (Map<String, Object>) properties.get("_attachments");
            Map<String, Object> editedAttachments = null;
            if (attachments != null) {
                for (String name : attachments.keySet()) {

                    Map<String, Object> attachment = new HashMap<String, Object>(
                            (Map<String, Object>) attachments.get(name));
                    attachment.put("name", name);
                    Map<String, Object> editedAttachment = functor.invoke(attachment);
                    if (editedAttachment == null) {
                        return false;  // block canceled
                    }
                    if (editedAttachment != attachment) {
                        if (editedProperties == null) {
                            // Make the document properties and _attachments dictionary mutable:
                            editedProperties = new HashMap<String, Object>(properties);
                            editedAttachments = new HashMap<String, Object>(attachments);
                            editedProperties.put("_attachments", editedAttachments);
                        }
                        editedAttachment.remove("name");
                        editedAttachments.put(name, editedAttachment);
                    }
                }
            }
            if (editedProperties != null) {
                setProperties(editedProperties);
                return true;
            }
            return false;
        }
    }

    public Map<String, Object> getAttachments() {
        Map<String, Object> props = getProperties();
        if (props != null && props.containsKey("_attachments")) {
            return (Map<String, Object>) props.get("_attachments");
        }
        return null;
    }

    /**
     * in CBL_Revision.m
     * - (id)objectForKeyedSubscript:(id)key
     */
    public Object getObject(String key) {
        return body != null ? body.getObject(key) : null;
    }
}
