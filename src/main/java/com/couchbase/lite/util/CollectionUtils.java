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
