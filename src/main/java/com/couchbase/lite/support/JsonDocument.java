package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.util.Log;

/**
 * A wrapper around a json byte array that will parse the data
 * as lat as possible
 */
public class JsonDocument {

    private final byte[] json;
    private Object cached = null;

    public JsonDocument(byte[] json) {
        this.json = json;
    }

    //Return a JSON object from the json data
    //If the Json starts with  '{' or a '[' then no parsing takes place and the
    //data is wrapped in a LazyJsonObject or a LazyJsonArray which will delay parsing until
    //values are requested
    public Object jsonObject() {

        if (json == null) {
            return null;
        }

        if (cached == null) {

            Object tmp = null;
            if (json[0] == '{') {
                tmp = new LazyJsonObject<String, Object>(json);
            } else if (json[0] == '[') {
                tmp = new LazyJsonArray<Object>(json);
            } else {
                try {
                    tmp = Manager.getObjectMapper().readValue(json, Object.class);
                } catch (Exception e) {
                    //cached will remain null
                    Log.w(Database.TAG, "Exception parsing json", e);
                }
            }

            cached = tmp;
        }
        return cached;
    }
}
