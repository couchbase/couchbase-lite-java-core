package com.couchbase.lite.store;

import com.couchbase.lite.cbforest.Collatable;
import com.couchbase.lite.cbforest.CollatableReader;
import com.couchbase.lite.cbforest.CollatableTypes;
import com.couchbase.lite.cbforest.Slice;
import com.couchbase.lite.util.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hideki on 8/25/15.
 */
public class ForestDBCollatorUtil {
    /**
     * utility method for Collatable
     */
    protected static Collatable add(Collatable collatable, Object obj) {
        if (collatable == null) return null;
        if (obj == null)
            return collatable.addNull();
        else if (obj instanceof String) {
            return collatable.add((String) obj);
        } else if (obj instanceof Boolean) {
            return collatable.add((Boolean) obj);
        } else if (obj instanceof Number) {
            return collatable.add(((Number) obj).doubleValue());
        } else if (obj instanceof Slice) {
            return collatable.add((Slice) obj);
        } else if (obj instanceof Collatable) {
            return collatable.add((Collatable) obj);
        } else if(obj instanceof List) {
            collatable.beginArray();
            List items = (List) obj;
            for (Object item : items)
                add(collatable, item);
            collatable.endArray();
            return collatable;
        }else if(obj instanceof Object[]){
            collatable.beginArray();
            Object[] items = (Object[]) obj;
            for (Object item : items)
                add(collatable, item);
            collatable.endArray();
            return collatable;
        } else if(obj instanceof Map){
            collatable.beginMap();
            Map map = (Map)obj;
            for(Object key : ((Map) obj).keySet()){
                add(collatable, key);
                add(collatable, map.get(key));
            }
            collatable.endMap();
            return collatable;
        } else {
            return collatable;
        }
    }

    /**
     * utility method for CollatableReader
     *
     * id CollatableReader::readNSObject()
     */
    public static Object read(CollatableReader cr){
        if(cr == null) return null;
        try {
            if (cr.peekTag() == CollatableTypes.Tag.kEndSequence.swigValue()) {

            } else if (cr.peekTag() == CollatableTypes.Tag.kNull.swigValue()) {
                return null;
            } else if (cr.peekTag() == CollatableTypes.Tag.kFalse.swigValue()) {
                return false;
            } else if (cr.peekTag() == CollatableTypes.Tag.kTrue.swigValue()) {
                return true;
            } else if (cr.peekTag() == CollatableTypes.Tag.kNegative.swigValue()) {
                return cr.readDouble();
            } else if (cr.peekTag() == CollatableTypes.Tag.kPositive.swigValue()) {
                return cr.readDouble();
            } else if (cr.peekTag() == CollatableTypes.Tag.kString.swigValue()) {
                return new String(cr.readString().getBuf());
            } else if (cr.peekTag() == CollatableTypes.Tag.kArray.swigValue()) {
                List<Object> result = new ArrayList<Object>();
                cr.beginArray();
                while(cr.peekTag() != 0)
                    result.add(read(cr));
                cr.endArray();
                return result;
            } else if (cr.peekTag() == CollatableTypes.Tag.kMap.swigValue()) {
                Map<String, Object> result = new HashMap<String, Object>();
                cr.beginMap();
                while(cr.peekTag() != 0){
                    String key = new String(cr.readString().getBuf());
                    result.put(key, read(cr));
                }
                cr.endMap();
                return result;
            } else if (cr.peekTag() == CollatableTypes.Tag.kSpecial.swigValue()) {

            } else if (cr.peekTag() == CollatableTypes.Tag.kError.swigValue()) {

            }
        }catch(Exception e){
            Log.e(ForestDBViewStore.TAG, "Error in ReadFromCollatableReader", e);
            return null;
        }
        return null;
    }
}
