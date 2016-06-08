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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

public class MultipartReader {

    private enum MultipartReaderState {
        kUninitialized,
        kAtStart,
        kInPrologue,
        kInBody,
        kInHeaders,
        kAtEnd,
        kFailed
    }

    private static final Charset utf8 = Charset.forName("UTF-8");
    private static final byte[] kCRLFCRLF = "\r\n\r\n".getBytes(utf8);
    private static final byte[] kEOM = "--".getBytes(utf8);

    private MultipartReaderState state = null;
    private CustomByteArrayOutputStream buffer = null;
    private String contentType = null;
    private byte[] boundary = null;
    private byte[] boundaryWithoutLeadingCRLF = null;
    private MultipartReaderDelegate delegate = null;
    public Map<String, String> headers = null;

    public MultipartReader(String contentType, MultipartReaderDelegate delegate) {
        this.contentType = contentType;
        this.delegate = delegate;
        this.buffer = new CustomByteArrayOutputStream(1024);
        this.state = MultipartReaderState.kAtStart;
        parseContentType();
    }

    public byte[] getBoundary() {
        return boundary;
    }

    public byte[] getBoundaryWithoutLeadingCRLF() {
        if (boundaryWithoutLeadingCRLF == null) {
            byte[] rawBoundary = getBoundary();
            boundaryWithoutLeadingCRLF = Arrays.copyOfRange(rawBoundary, 2, rawBoundary.length);
        }
        return boundaryWithoutLeadingCRLF;
    }

    public boolean finished() {
        return state == MultipartReaderState.kAtEnd;
    }

    private static boolean memcmp(byte[] array1, byte[] array2, int len) {
        boolean equals = true;
        for (int i = 0; i < len; i++) {
            if (array1[i] != array2[i])
                equals = false;
        }
        return equals;
    }

    public Range searchFor(byte[] pattern, int start) {
        KMPMatch searcher = new KMPMatch();
        int matchIndex = searcher.indexOf(buffer.buf(), buffer.count(), pattern, start);
        if (matchIndex != -1)
            return new Range(matchIndex, pattern.length);
        else
            return new Range(matchIndex, 0);
    }

    public void parseHeaders(String headersStr) {
        headers = new HashMap<String, String>();
        if (headersStr != null && headersStr.length() > 0) {
            headersStr = headersStr.trim();
            StringTokenizer tokenizer = new StringTokenizer(headersStr, "\r\n");
            while (tokenizer.hasMoreTokens()) {
                String header = tokenizer.nextToken();
                if (!header.contains(":"))
                    throw new IllegalArgumentException("Missing ':' in header line: " + header);
                int colon = header.indexOf(':');
                String key = header.substring(0, colon).trim();
                String value = header.substring(colon + 1).trim();
                headers.put(key, value);
            }
        }
    }

    private void deleteUpThrough(int location) {
        if (location <= 0) return;
        byte[] b = buffer.toByteArray();
        buffer.reset();
        buffer.write(b, location, b.length - location);
    }

    private void trimBuffer() {
        int bufLen = buffer.count();
        int boundaryLen = getBoundary().length;
        if (bufLen > boundaryLen) {
            delegate.appendToPart(buffer.buf(), 0, bufLen - boundaryLen);
            deleteUpThrough(bufLen - boundaryLen);
        }
    }

    public void appendData(byte[] data) {
        appendData(data, 0, data.length);
    }

    public void appendData(byte[] data, int off, int len) {
        if (buffer == null)
            return;
        if (len == 0)
            return;

        buffer.write(data, off, len);
        MultipartReaderState nextState;
        do {
            nextState = MultipartReaderState.kUninitialized;
            int bufLen = buffer.count();
            switch (state) {
                case kAtStart: {
                    // The entire message might start with a boundary without a leading CRLF.
                    byte[] boundaryWithoutLeadingCRLF = getBoundaryWithoutLeadingCRLF();
                    if (bufLen >= boundaryWithoutLeadingCRLF.length) {
                        if (memcmp(buffer.buf(), boundaryWithoutLeadingCRLF, boundaryWithoutLeadingCRLF.length)) {
                            deleteUpThrough(boundaryWithoutLeadingCRLF.length);
                            nextState = MultipartReaderState.kInHeaders;
                        } else {
                            nextState = MultipartReaderState.kInPrologue;
                        }
                    }
                    break;
                }
                case kInPrologue:
                case kInBody: {
                    // Look for the next part boundary in the data we just added and the ending bytes of
                    // the previous data (in case the boundary string is split across calls)
                    if (bufLen < boundary.length) {
                        break;
                    }
                    int start = Math.max(0, bufLen - data.length - boundary.length);
                    Range r = searchFor(boundary, start);
                    if (r.getLength() > 0) {
                        if (state == MultipartReaderState.kInBody) {
                            delegate.appendToPart(buffer.buf(), 0, r.getLocation());
                            delegate.finishedPart();
                        }
                        deleteUpThrough(r.getLocation() + r.getLength());
                        nextState = MultipartReaderState.kInHeaders;
                    } else {
                        trimBuffer();
                    }
                    break;
                }
                case kInHeaders: {
                    // First check for the end-of-message string ("--" after separator):
                    if (bufLen >= kEOM.length && memcmp(buffer.buf(), kEOM, kEOM.length)) {
                        state = MultipartReaderState.kAtEnd;
                        close();
                        return;
                    }
                    // Otherwise look for two CRLFs that delimit the end of the headers:
                    Range r = searchFor(kCRLFCRLF, 0);
                    if (r.getLength() > 0) {
                        String headersString = new String(buffer.buf(), 0, r.getLocation(), utf8);
                        parseHeaders(headersString);
                        deleteUpThrough(r.getLocation() + r.getLength());
                        delegate.startedPart(headers);
                        nextState = MultipartReaderState.kInBody;
                    }
                    break;
                }
                default: {
                    throw new IllegalStateException("Unexpected data after end of MIME body");
                }
            }
            if (nextState != MultipartReaderState.kUninitialized)
                state = nextState;
        } while (nextState != MultipartReaderState.kUninitialized && buffer.count() > 0);
    }

    private void close() {
        if (buffer != null) {
            try {
                buffer.close();
            } catch (IOException e) {
            }
        }
        buffer = null;
        boundary = null;
        boundaryWithoutLeadingCRLF = null;
    }

    private void parseContentType() {
        StringTokenizer tokenizer = new StringTokenizer(contentType, ";");
        boolean first = true;
        while (tokenizer.hasMoreTokens()) {
            String param = tokenizer.nextToken().trim();
            if (first == true) {
                if (!param.startsWith("multipart/"))
                    throw new IllegalArgumentException(contentType + " does not start with multipart/");
                first = false;
            } else {
                if (param.startsWith("boundary=")) {
                    String tempBoundary = param.substring(9);
                    if (tempBoundary.startsWith("\"")) {
                        if (tempBoundary.length() < 2 || !tempBoundary.endsWith("\""))
                            throw new IllegalArgumentException(contentType + " is not valid");
                        tempBoundary = tempBoundary.substring(1, tempBoundary.length() - 1);
                    }
                    if (tempBoundary.length() < 1)
                        throw new IllegalArgumentException(contentType + " has zero-length boundary");
                    tempBoundary = String.format(Locale.ENGLISH, "\r\n--%s", tempBoundary);
                    boundary = tempBoundary.getBytes(Charset.forName("UTF-8"));
                    break;
                }
            }
        }
    }
}

