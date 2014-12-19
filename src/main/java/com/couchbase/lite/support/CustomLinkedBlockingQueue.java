package com.couchbase.lite.support;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hideki on 12/17/14.
 */
public class CustomLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {
    private BlockingQueueListener listener = null;

    public CustomLinkedBlockingQueue() {
    }

    public CustomLinkedBlockingQueue(BlockingQueueListener listener) {
        this.listener = listener;
    }

    public CustomLinkedBlockingQueue(int capacity) {
        super(capacity);
    }

    public CustomLinkedBlockingQueue(Collection c) {
        super(c);
    }

    public BlockingQueueListener getListener() {
        return listener;
    }

    public void setListener(BlockingQueueListener listener) {
        this.listener = listener;
    }

    @Override
    public void put(E e) throws InterruptedException {
        super.put(e);

        if(listener != null) {
            listener.changed(BlockingQueueListener.EventType.PUT, e, this);
        }
    }

    @Override
    public boolean add(E e) {
        boolean b = super.add(e);
        if(listener != null) {
            listener.changed(BlockingQueueListener.EventType.ADD, e, this);
        }
        return b;
    }

    @Override
    public E take() throws InterruptedException {
        E e = super.take();

        if(listener != null) {
            listener.changed(BlockingQueueListener.EventType.TAKE, e, this);
        }

        return e;
    }
}
