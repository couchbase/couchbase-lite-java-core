package com.couchbase.lite.support;

import java.util.Queue;
import java.util.concurrent.Future;

/**
 * Created by hideki on 5/21/15.
 */
public interface CustomFuture<V> extends Future<V> {
    void setQueue(Queue queue);
}
