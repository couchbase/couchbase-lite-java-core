package com.couchbase.lite.store;


import com.couchbase.lite.Manager;
import com.couchbase.lite.cbforest.RevIDBuffer;
import com.couchbase.lite.cbforest.Revision;
import com.couchbase.lite.cbforest.Slice;
import com.couchbase.lite.cbforest.VectorRevision;
import com.couchbase.lite.cbforest.VersionedDocument;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.util.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hideki on 11/25/14.
 * <p/>
 * see CBLForestBridge.h and CBLForestBridge.mm
 */
public class ForestBridge {
    public final static String TAG = ForestBridge.class.getSimpleName();

    /**
     * in CBLForestBridge.mm
     * static NSData* dataOfNode(const Revision* rev)
     */
    public static byte[] dataOfNode(Revision rev) {
        //TODO:
        //if (rev->inlineBody().buf)
        //    return rev->inlineBody().uncopiedNSData();
        return rev.readBody().getBuf();
    }

    /**
     * in CBLForestBridge.m
     * + (CBL_MutableRevision*) revisionObjectFromForestDoc: (VersionedDocument&)doc
     * revID: (NSString*)revID
     * withBody: (BOOL)withBody
     */
    public static RevisionInternal revisionObjectFromForestDoc(
            VersionedDocument doc,
            String revID,
            boolean withBody)
            throws Exception
    {
        RevisionInternal rev;
        String docID = new String(doc.getDocID().getBuf());
        if (doc.revsAvailable()) {
            com.couchbase.lite.cbforest.Revision revNode;
            if (revID != null) {
                revNode = doc.get(new RevIDBuffer(new Slice(revID.getBytes())));
            } else {
                revNode = doc.currentRevision();
                if (revNode != null)
                    revID = new String(revNode.getRevID().getBuf());
            }
            if (revNode == null)
                return null;
            rev = new RevisionInternal(docID, revID, revNode.isDeleted());
            rev.setSequence(revNode.getSequence().longValue());
        } else {
            rev = new RevisionInternal(docID, new String(doc.getRevID().getBuf()), doc.isDeleted());
            rev.setSequence(doc.getSequence().longValue());
        }
        if (withBody && !loadBodyOfRevisionObject(rev, doc))
            return null;
        return rev;
    }


    /**
     * in CBLForestBridge.m
     * + (BOOL) loadBodyOfRevisionObject: (CBL_MutableRevision*)rev
     * doc: (VersionedDocument&)doc
     */
    public static boolean loadBodyOfRevisionObject(
            RevisionInternal rev,
            VersionedDocument doc)
            throws Exception
    {
        Revision revNode = doc.get(new RevIDBuffer(new Slice(rev.getRevID().getBytes())));
        if (revNode == null)
            return false;
        byte[] json = dataOfNode(revNode);
        if (json == null)
            return false;
        rev.setSequence(revNode.getSequence().longValue());
        rev.setJSON(json);
        return true;
    }

    /**
     * in CBLForestBridge.m
     * + (NSMutableDictionary*) bodyOfNode: (const Revision*)rev
     *
     * Note: Unable to downcast from RevTree to VersionedDocument
     *        Instead of downcast, add VersionedDocument parameter
     */
    public static Map<String, Object> bodyOfNode(Revision rev, VersionedDocument doc){

        byte[] json = dataOfNode(rev);
        if(json == null)
            return null;

        Map<String, Object> properties = null;
        try {
            properties = Manager.getObjectMapper().readValue(json, Map.class);
        } catch (IOException e) {
            Log.e(TAG, "Error in bodyOfNode()", e);
            return null;
        }

        String docID = new String(doc.getDocID().getBuf());
        String revID = new String(rev.getRevID().getBuf());
        properties.put("_id", docID);
        properties.put("_rev", revID);
        if(rev.isDeleted())
            properties.put("_deleted", true);
        return properties;
    }



    /**
     * in CBLForestBridge.m
     * + (NSArray*) getCurrentRevisionIDs: (VersionedDocument&)doc
     */
    public static List<String> getCurrentRevisionIDs(VersionedDocument doc) {
        List<String> currentRevIDs = new ArrayList<String>();
        VectorRevision revs = doc.currentRevisions();
        for (int i = 0; i < revs.size(); i++) {
            Revision rev = revs.get(i);
            if (!rev.isDeleted()) {
                String revID = new String(rev.getRevID().getBuf());
                currentRevIDs.add(revID);
            }
        }
        return currentRevIDs;
    }


    /**
     * in CBLForestBridge.m
     * + (NSArray*) getRevisionHistory: (const Revision*)revNode
     *
     * Note: Unable to downcast from RevTree to VersionedDocument
     * Instead of downcast, add docID parameter
     */
    public static List<RevisionInternal> getRevisionHistory(String docID, Revision revNode) {
        List<RevisionInternal> history = new ArrayList<RevisionInternal>();
        for (; revNode != null; revNode = revNode.getParent()) {

            String revID = revNode.getRevID().toString();
            boolean deleted = revNode.isDeleted();
            RevisionInternal rev = new RevisionInternal(docID, revID, deleted);
            rev.setMissing(!revNode.isBodyAvailable());
            history.add(rev);
        }
        return history;
    }


    /*
    public static Map<String,Object> getRevisionHistoryDictStartingFromAnyAncestor(String docID, Revision revNode, List<String> ancestorRevIDs){
        List<RevisionInternal> history = getRevisionHistory(docID, revNode);
        if (ancestorRevIDs != null && ancestorRevIDs.size() > 0) {
            int n = history.size();
            for (int i = 0; i < n; ++i) {
                if (ancestorRevIDs.contains(history.get(i).getRevId())) {
                    history = history.subList(0, i+1);
                    break;
                }
            }
        }
        return DatabaseUtil.makeRevisionHistoryDict(history);
    }
    */
    /**
     * in CBForestBridge.m
     * static NSDictionary* makeRevisionHistoryDict(NSArray* history)
     * moved to DatabaseUtil.java
     */
    //public static Map<String,Object> DatabaseUtil.makeRevisionHistoryDict(List<RevisionInternal> history);


    /**
     * + (void) addContentProperties: (CBLContentOptions)options
     *                          into: (NSMutableDictionary*)dst
     *                           rev: (const Revision*)rev
     *
     *  Note: Unable to downcast from RevTree to VersionedDocument
     *        Instead of downcast, add VersionedDocument parameter
     */
    /*
    @InterfaceAudience.Private
    public static void addContentProperties(EnumSet<Database.TDContentOptions> options, Map<String,Object> dst, com.couchbase.lite.cbforest.Revision rev, VersionedDocument doc) {

        String revID = new String(rev.getRevID().getBuf());
        assert(revID!=null);
        // I am not sure if downcast is possible with JNI
        //com.couchbase.cbforest.VersionedDocument doc = (com.couchbase.cbforest.VersionedDocument)rev.getOwner();
        String docID = new String(doc.getDocID().getBuf());
        dst.put("_id", docID);
        dst.put("_rev", revID);
        if(rev.isDeleted())
            dst.put("_deleted", true);

        // Get more optional stuff to put in the properties:
        if(options.contains(Database.TDContentOptions.TDIncludeLocalSeq)) {
            Long localSeq = rev.getSequence().longValue();
            dst.put("_local_seq", localSeq);
        }

        // TODO: Keep implement!!!!!
        if(options.contains(Database.TDContentOptions.TDIncludeRevs)) {
            Long localSeq = rev.getSequence().longValue();
            dst.put("_local_seq", localSeq);
            //Map<String,Object> revHistory = getRevisionHistoryDict(rev);
        }
    }
    */

    /**
     * Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.
     * <p/>
     * Notes: 1) This code is from Database.java
     * 2) in iOS - CBLJSON.m - + (NSData*) appendDictionary: (NSDictionary*)dict
     * toJSONDictionaryData: (NSData*)json
     */
    /*
    public static byte[] appendDictToJSON(Map<String, Object> dict, byte[] json) {
        if (dict.size() == 0) {
            return json;
        }

        byte[] extraJSON = null;
        try {
            extraJSON = Manager.getObjectMapper().writeValueAsBytes(dict);
        } catch (Exception e) {
            Log.e(TAG, "Error convert extra JSON to bytes", e);
            return null;
        }

        int jsonLength = json.length;
        int extraLength = extraJSON.length;
        if (jsonLength == 2) { // Original JSON was empty
            return extraJSON;
        }
        byte[] newJson = new byte[jsonLength + extraLength - 1];
        System.arraycopy(json, 0, newJson, 0, jsonLength - 1);  // Copy json w/o trailing '}'
        newJson[jsonLength - 1] = ',';  // Add a ','
        System.arraycopy(extraJSON, 1, newJson, jsonLength, extraLength - 1);
        return newJson;
    }
    */
}