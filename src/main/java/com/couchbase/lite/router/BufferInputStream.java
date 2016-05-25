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
import java.io.InputStream;

public class BufferInputStream extends InputStream {

    private BufferOutputStream os;

    public BufferInputStream(BufferOutputStream os) {
        this.os = os;
    }

    public int read() throws IOException {
        ByteBuffer buffer = os.getBuffer();
        synchronized (buffer) {
            while (buffer.isEmpty()) {
                if (os.isClosed()) {
                    return -1;
                }
                try {
                    buffer.wait(1000); // 1sec
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return (int) buffer.pop();
        }
    }

    public int read(byte[] bytes, int offset, int length) throws IOException {
        if (bytes == null) {
            throw new NullPointerException();
        }
        if (offset + length > bytes.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset can not be negative");
        }
        ByteBuffer buffer = os.getBuffer();
        synchronized (buffer) {
            while (buffer.isEmpty()) {
                if (os.isClosed()) {
                    return -1;
                }
                try {
                    buffer.wait(1000); // 1sec
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return os.getBuffer().pop(bytes, offset, length);
        }
    }
}
