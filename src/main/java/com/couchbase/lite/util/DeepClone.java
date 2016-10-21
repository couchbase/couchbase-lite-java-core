//
// Copyright (c) 2016 Couchbase, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
//

package com.couchbase.lite.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Deep clone instead of shallow cloning.
 */
public final class DeepClone {
    private DeepClone() {
    }

    public static <T> T deepClone(final T input) {
        if (input == null) {
            return input;
        } else if (input instanceof Map<?, ?>) {
            return (T) deepCloneMap((Map<?, ?>) input);
        } else if (input instanceof Collection<?>) {
            return (T) deepCloneCollection((Collection<?>) input);
        } else if (input instanceof Object[]) {
            return (T) deepCloneObjectArray((Object[]) input);
        } else if (input.getClass().isArray()) {
            return (T) clonePrimitiveArray(input);
        }
        return input;
    }

    private static Object clonePrimitiveArray(final Object input) {
        final int length = Array.getLength(input);
        final Object copy = Array.newInstance(input.getClass().getComponentType(), length);
        System.arraycopy(input, 0, copy, 0, length);
        return copy;
    }

    private static <E> E[] deepCloneObjectArray(final E[] input) {
        final E[] clone = (E[]) Array.newInstance(input.getClass().getComponentType(), input.length);
        for (int i = 0; i < input.length; i++)
            clone[i] = deepClone(input[i]);
        return clone;
    }

    private static <E> Collection<E> deepCloneCollection(final Collection<E> input) {
        Collection<E> clone;
        if (input instanceof LinkedList<?>)
            clone = new LinkedList<E>();
        else if (input instanceof SortedSet<?>)
            clone = new TreeSet<E>();
        else if (input instanceof Set)
            clone = new HashSet<E>();
        else
            clone = new ArrayList<E>();

        for (E item : input)
            clone.add(deepClone(item));
        return clone;
    }

    private static <K, V> Map<K, V> deepCloneMap(final Map<K, V> map) {
        Map<K, V> clone;
        if (map instanceof LinkedHashMap<?, ?>)
            clone = new LinkedHashMap<K, V>();
        else if (map instanceof TreeMap<?, ?>)
            clone = new TreeMap<K, V>();
        else
            clone = new HashMap<K, V>();

        for (Map.Entry<K, V> entry : map.entrySet())
            clone.put(deepClone(entry.getKey()), deepClone(entry.getValue()));
        return clone;
    }
}
