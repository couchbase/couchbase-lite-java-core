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
package com.couchbase.lite.router;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferOutputStream extends OutputStream {

    ByteBuffer buffer = new ByteBuffer();
    AtomicBoolean closed = new AtomicBoolean(false);

    public void write(int i) throws IOException {
        if(!isClosed()) {
            synchronized (buffer) {
                buffer.push((byte) i);
                buffer.notify();
            }
        } else {
            throw new IOException("Can't write to closed stream.");
        }
    }

    ByteBuffer getBuffer() {
        return buffer;
    }

    public void close() {
        closed.set(true);
        synchronized(buffer) {
            buffer.notify();
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

}
