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

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E e = super.poll(timeout, unit);
        if (listener != null) {
            listener.changed(BlockingQueueListener.EventType.POLL, e, this);
        }
        return e;
    }

    @Override
    public E poll() {
        E e = super.poll();
        if (listener != null) {
            listener.changed(BlockingQueueListener.EventType.POLL, e, this);
        }
        return e;
    }
}
