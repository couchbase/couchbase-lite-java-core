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

/**
 * Created by pum on 29.06.2015.
 */
public class ByteBuffer {

    private static final int CHUNK_SIZE = 2048;

    private byte[] buffer = null;
    private int readIndex = 0;
    private int writeIndex = 0;

    public ByteBuffer() {
        buffer = new byte[CHUNK_SIZE];
    }

    public void push(byte data) {
        if(writeIndex == buffer.length) {
            resize();
        }
        buffer[writeIndex++] = data;
    }

    public void push(byte[] data) {
        push(data, 0, data.length);
    }

    public void push(byte[] data, int offset, int length) {
        if(data == null) {
            throw new NullPointerException();
        }
        if(offset + length > data.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if(offset < 0) {
            throw new IllegalArgumentException("offset can not be negative");
        }
        for(int i = 0; i < length; i++) {
            push(data[offset + i]);
        }
    }

    public Byte pop() {
        if(!isEmpty()) {
            return buffer[readIndex++];
        }
        return null;
    }

    public int pop(byte[] array) {
        return pop(array, 0, array.length);
    }

    public int pop(byte[] array, int offset, int length) {
        if(array == null) {
            throw new NullPointerException();
        }
        if(offset + length > array.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if(offset < 0) {
            throw new IllegalArgumentException("offset can not be negative");
        }
        int count = 0;
        while(count < length && !isEmpty()) {
            array[offset + count++] = buffer[readIndex++];
        }
        return count;
    }

    public boolean isEmpty() {
        return readIndex >= writeIndex;
    }

    private void resize() {
        byte[] temp = buffer;
        int count = 0;
        if(readIndex < (CHUNK_SIZE / 2)) {
            buffer = new byte[temp.length + CHUNK_SIZE];
        }
        for(count = 0; readIndex + count < writeIndex; count++) {
            buffer[count] = temp[readIndex + count];
        }
        readIndex = 0;
        writeIndex = count;
    }

}
