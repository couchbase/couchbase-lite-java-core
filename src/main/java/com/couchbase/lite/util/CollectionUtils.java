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
package com.couchbase.lite.util;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by andy on 15/04/2014.
 */
public class CollectionUtils {
    public static <T> Collection<T> filter(Collection<T> target, Predicate<T> predicate) {
        Collection<T> result = new ArrayList<T>();
        for (T element : target) {
            if (predicate.apply(element)) {
                result.add(element);
            }
        }
        return result;
    }

    public static <T, U> Collection<U> transform(Collection<T> target, Functor<T, U> functor) {
        Collection<U> result = new ArrayList<U>();
        for (T element : target) {
            U mapped = functor.invoke(element);
            if (mapped != null) {
                result.add(mapped);
            }
        }
        return result;
    }

    public interface Predicate<T> {
        boolean apply(T type);
    }

    public interface Functor<T, U> {
        U invoke(T source);
    }
}
