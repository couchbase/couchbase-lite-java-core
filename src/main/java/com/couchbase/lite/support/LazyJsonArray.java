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

/**
 * A wrapper object for data representing a JSON array, parsing of the JSON
 * data is delayed until it is accessed via one of the List methods
 */
import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.util.Log;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LazyJsonArray<T> extends AbstractList<T> {

    private boolean parsed = false;
    private byte[] json;

    private List<T> cache = new ArrayList<T>();
    private Iterator<T> cacheIterator;

    public LazyJsonArray(byte[] json) {
        if(json[0] != '[') {
            throw new IllegalArgumentException("data must represent a JSON array");
        }
        this.json = json;
    }

    @Override
    public Iterator<T> iterator() {
        if(parsed) {
            return cache.iterator();
        } else {
            parseJson();
            return cache.iterator();
        }
    }

    @Override
    public T get(int index) {
        if(parsed) {
            return cache.get(index);
        } else {
            parseJson();
            return cache.get(index);
        }
    }

    @Override
    public int size() {
        if(parsed) {
            return cache.size();
        } else {
            parseJson();
            return cache.size();
        }
    }

    // the following methods in AbstractList use #iterator(). Overwrite them to make sure they use the
    // cached version
    @Override
    public boolean contains(Object o) {
        if(parsed) {
            return cache.contains(o);
        } else {
            parseJson();
            return cache.contains(o);
        }
    }
    @Override
    public Object[] toArray() {
        if(parsed) {
            return cache.toArray();
        } else {
            parseJson();
            return cache.toArray();
        }
    }
    public <S> S[] toArray(S[] a) {
        if(parsed) {
            return cache.toArray(a);
        } else {
            parseJson();
            return cache.toArray(a);
        }
    }
    @Override
    public int hashCode() {
        if(parsed) {
            return cache.hashCode();
        } else {
            parseJson();
            return cache.hashCode();
        }
    }

    private void parseJson() {
        if(parsed) {
            return;
        }

        try {
            List<T> parsedvalues  = (List<T>) Manager.getObjectMapper().readValue(json, Object.class);
            //Merge parsed values into List, overwriting the values for duplicate keys
            parsedvalues.addAll(cache);
            cache = parsedvalues;
        } catch (Exception e) {
            Log.e(Database.TAG, this.getClass().getName()+": Failed to parse Json data: ", e);
        } finally {
            parsed = true;
            json = null;
        }
    }
}