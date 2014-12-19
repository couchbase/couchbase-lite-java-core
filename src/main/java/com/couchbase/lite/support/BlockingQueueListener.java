package com.couchbase.lite.support;

import java.util.concurrent.BlockingQueue;

/**
 * Created by hideki on 12/17/14.
 */
public interface BlockingQueueListener<E> {
    public enum EventType{ ADD, PUT, TAKE }
    public void changed(EventType type, E e, BlockingQueue<E> queue);
}
