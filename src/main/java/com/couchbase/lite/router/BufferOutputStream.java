package com.couchbase.lite.router;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferOutputStream extends OutputStream {

    ByteBuffer buffer = new ByteBuffer();
    AtomicBoolean closed = new AtomicBoolean(false);

    public void write(int i) throws IOException {
        if(!isClosed()) {
            synchronized (buffer) {
                buffer.push((byte) i);
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
    }

    public boolean isClosed() {
        return closed.get();
    }

}
