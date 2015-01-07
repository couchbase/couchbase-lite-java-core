package com.couchbase.lite;

/**
 * Created by hideki on 1/7/15.
 */
public interface Predicate<T> {
    boolean apply(T type);
}
