package com.couchbase.lite.router;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

public class BufferInputStream extends InputStream {

    private BufferOutputStream os;

    public BufferInputStream(BufferOutputStream os) {
        this.os = os;
    }

    public int read() throws IOException {
        while(os.getBuffer().isEmpty()) {
            if(os.isClosed()) {
                if(os.getBuffer().isEmpty()) {
                    return -1;
                }
            }
        }
        synchronized (os.getBuffer()) {
            return (int)os.getBuffer().pop();
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
        while(os.getBuffer().isEmpty()) {
            if(os.isClosed()) {
                if(os.getBuffer().isEmpty()) {
                    return -1;
                }
            }
        }
        synchronized (os.getBuffer()) {
            return os.getBuffer().pop(bytes, offset, length);
        }
    }
}
