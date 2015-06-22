package com.couchbase.lite.router;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

/**
 * Created by florian on 19.06.2015.
 */
public class BufferInputStream extends InputStream {

    private BufferOutputStream os;

    public BufferInputStream(BufferOutputStream os) {
        this.os = os;
    }

    public int read() throws IOException {
        while(os.getBuffer().size() == 0) {
            if(os.isClosed()) {
                if(os.getBuffer().size() == 0) {
                    return -1;
                }
            }
        }
        synchronized (os.getBuffer()) {
            return (int)os.getBuffer().removeFirst();
        }
    }

    public int read(byte[] bytes, int offset, int length) throws IOException {
        int firstByte = read();
        int count = 1;
        if(firstByte < 0) {
            return -1;
        }
        bytes[offset] = (byte)firstByte;
        while(count <= length && os.getBuffer().size() > 0) {
            bytes[count++] = (byte)read();
        }
        return count;
    }
}
