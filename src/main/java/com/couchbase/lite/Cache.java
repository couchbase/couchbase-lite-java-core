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
package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.support.WeakValueHashMap;
import com.couchbase.lite.util.LruCache;

/**
 * An in-memory object cache.
 *
 * It keeps track of all added objects as long as anything else has retained them,
 * and it keeps a certain number of recently-accessed objects with no external references.
 * It's intended for use by a parent resource, to cache its children.
 *
 * @exclude
 */
@InterfaceAudience.Private
public class Cache<K,V> {

    private static final int DEFAULT_RETAIN_LIMIT = 50;

    // how many items to retain strong references to
    int retainLimit = DEFAULT_RETAIN_LIMIT;

    // the underlying strong reference cache
    private LruCache<K, V> strongReferenceCache;

    // the underlying weak reference cache
    private WeakValueHashMap<K, V> weakReferenceCache;


    public Cache() {
        this(DEFAULT_RETAIN_LIMIT);
    }

    public Cache(int retainLimit) {
        this.retainLimit = retainLimit;
        strongReferenceCache = new LruCache<K, V>(this.retainLimit);
        weakReferenceCache = new WeakValueHashMap<K, V>();
    }

    public V put(K key, V value) {
        strongReferenceCache.put(key, value);
        weakReferenceCache.put(key, value);
        return value;
    }

    public V get(K key) {

        V value = null;
        if (weakReferenceCache.containsKey(key)) {
            value = weakReferenceCache.get(key);
        }

        if (value != null && strongReferenceCache.get(key) == null) {
            strongReferenceCache.put(key, value);  // re-add doc to NSCache since it's recently used
        }
        return value;
    }

    public V remove(K key) {
        V removedStrongValue = null;
        V removedWeakValue = null;
        removedStrongValue = strongReferenceCache.remove(key);
        removedWeakValue = weakReferenceCache.remove(key);
        if (removedStrongValue != null) {
            return removedStrongValue;
        }
        if (removedWeakValue != null) {
            return removedWeakValue;
        }
        return null;
    }

    public void clear() {
        strongReferenceCache.evictAll();
        weakReferenceCache.clear();
    }

    public V resourceWithCacheKeyDontRecache(K key) {
        return weakReferenceCache.get(key);
    }
}
