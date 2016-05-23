/**
 * Copyright (c) 2016 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
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
                    // NOTE: This if-else condition is for Jackson 2.5.0
                    // json variable is byte[] which is from Cursor.getBlob().
                    // And json byte array is ended with '\0'.
                    // '\0' causes parsing problem with Jackson 2.5.0 that we upgraded Feb 24, 2015.
                    // We did not observe this problem with Jackson 1.9.2 that we used before.
                    if(json.length > 0 && json[json.length - 1] == 0) {
                        tmp = Manager.getObjectMapper().readValue(json, 0, json.length - 1, Object.class);
                    }
                    else {
                        tmp = Manager.getObjectMapper().readValue(json, Object.class);
                    }
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
