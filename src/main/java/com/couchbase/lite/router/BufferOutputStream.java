package com.couchbase.lite.router;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by florian on 19.06.2015.
 */
public class BufferOutputStream extends OutputStream {

    LinkedList<Byte> buffer = new LinkedList<Byte>();
    AtomicBoolean closed = new AtomicBoolean(false);

    public void write(int i) throws IOException {
        synchronized (buffer) {
            buffer.addLast((byte)i);
        }
    }

    LinkedList<Byte> getBuffer() {
        return buffer;
    }

    public void close() {
        closed.set(true);
    }

    public boolean isClosed() {
        return closed.get();
    }

}
