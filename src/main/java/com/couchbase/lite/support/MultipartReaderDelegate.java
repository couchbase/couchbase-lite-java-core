package com.couchbase.lite.support;

import java.util.Map;

public interface MultipartReaderDelegate {
    void startedPart(Map<String, String> headers);

    // obsolete
    void appendToPart(byte[] data);

    void appendToPart(final byte[] data, int off, int len);

    void finishedPart();
}
