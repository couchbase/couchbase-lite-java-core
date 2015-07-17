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
            while(buffer.isEmpty()) {
                if(os.isClosed()) {
                    return -1;
                }
                try {
                    buffer.wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return (int)buffer.pop();
        }
    }

    public int read(byte[] bytes, int offset, int length) throws IOException {
        if(bytes == null) {
            throw new NullPointerException();
        }
        if(offset + length > bytes.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if(offset < 0) {
            throw new IllegalArgumentException("offset can not be negative");
        }
        ByteBuffer buffer = os.getBuffer();
        synchronized (buffer) {
            while(buffer.isEmpty()) {
                if(os.isClosed()) {
                    return -1;
                }
                try {
                    buffer.wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return os.getBuffer().pop(bytes, offset, length);
        }
    }
}
