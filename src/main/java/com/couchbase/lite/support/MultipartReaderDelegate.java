package com.couchbase.lite.support;

import java.util.Map;

public interface MultipartReaderDelegate {

    public void startedPart(Map<String, String> headers);

    // obsolete
    public void appendToPart(byte[] data);
    public void appendToPart(final byte[] data, int off, int len);

    public void finishedPart();

}
