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

/**
 * Created by hideki on 9/30/16.
 */
public class RefCounter {
    private int refCount = 0;

    public synchronized void retain() {
        refCount++;
    }

    public synchronized void release() {
        if (refCount == 0)
            throw new IllegalStateException("Reference counter is set to zero -- cannot release!");
        if (--refCount == 0) {
            notifyAll();
        }
    }

    public synchronized int count() {
        return refCount;
    }

    public synchronized void await() {
        while (count() != 0) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
    }
}
