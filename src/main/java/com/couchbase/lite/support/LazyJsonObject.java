package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.util.Log;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/*
 * A wrapper object for data representing a JSON object, parsing of the JSON
 * data is delayed until it is accessed via one of the Map methods
 * */
public class LazyJsonObject<K, V> extends AbstractMap<K, V> {

    private boolean parsed = false;
    private byte[] json;
    private Map<K, V> cache = new HashMap<K, V>();

    public LazyJsonObject(byte[] json) {
        if (json[0] != '{') {
            throw new IllegalArgumentException("data must represent a JSON Object");
        }
        this.json = json;
    }

    @Override
    public V put(K key, V value) {
        //value for key takes priority over json properties even if
        //json has not been parsed yet
        return cache.put(key, value);
    }

    @Override
    public V get(Object key) {
        if (cache.containsKey(key)) {
            return cache.get(key);
        } else {
            parseJson();
            return cache.get(key);
        }
    }

    @Override
    public V remove(Object key) {
        if (cache.containsKey(key)) {
            return cache.remove(key);
        } else {
            parseJson();
            return cache.remove(key);
        }
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        if (cache.containsKey(key)) {
            return cache.containsKey(key);
        } else {
            parseJson();
            return cache.containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        if (cache.containsValue(value)) {
            return cache.containsValue(value);
        } else {
            parseJson();
            return cache.containsValue(value);
        }
    }

    @Override
    public Set<K> keySet() {
        parseJson();
        return cache.keySet();
    }

    @Override
    public int size() {
        parseJson();
        return cache.size();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        parseJson();
        return cache.entrySet();
    }

    public Collection<V> values() {
        parseJson();
        return cache.values();
    }

    private void parseJson() {
        if (parsed) {
            return;
        }

        try {
            Map<K, V> parsedprops = (Map<K, V>) Manager.getObjectMapper().readValue(json, Object.class);
            //Merge parsed properties into map, overwriting the values for duplicate keys
            parsedprops.putAll(cache);
            cache = parsedprops;
        } catch (Exception e) {
            Log.e(Database.TAG, this.getClass().getName() + ": Failed to parse Json data: ", e);
        } finally {
            parsed = true;
            json = null;
        }
    }
}