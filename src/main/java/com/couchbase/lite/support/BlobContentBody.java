/**
 * Created by Hideki Itakura on 4/4/16.
 * <p/>
 * Copyright (c) 2016 Couchbase, Inc All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.couchbase.lite.support;

import com.couchbase.lite.BlobKey;
import com.couchbase.lite.BlobStore;
import com.couchbase.lite.util.Log;
import com.couchbase.org.apache.http.entity.mime.MIME;
import com.couchbase.org.apache.http.entity.mime.content.AbstractContentBody;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BlobContentBody extends AbstractContentBody {

    private final BlobStore blobStore;
    private final BlobKey blobKey;
    private final String filename;
    private final long contentLength;
    private final String contentEncoding;

    public BlobContentBody(final BlobStore blobStore,
                           final BlobKey blobKey,
                           final String mimeType,
                           final String filename,
                           final long contentLength,
                           final String contentEncoding) {
        super(mimeType);
        if (blobStore == null)
            throw new IllegalArgumentException("BlobStore may not be null");
        if (blobKey == null)
            throw new IllegalArgumentException("BlobKey may not be null");
        if (contentLength < -1)
            throw new IllegalArgumentException("Content length must be >= -1");
        this.blobStore = blobStore;
        this.blobKey = blobKey;
        this.filename = filename;
        this.contentLength = contentLength;
        this.contentEncoding = contentEncoding;
    }

    public BlobContentBody(final BlobStore blobStore,
                           final BlobKey blobKey,
                           final String mimeType,
                           final String filename,
                           final String contentEncoding) {
        this(blobStore, blobKey, mimeType, filename, -1L, contentEncoding);
    }

    public void writeTo(OutputStream out) throws IOException {
        if (out == null)
            throw new IllegalArgumentException("Output stream may not be null");

        InputStream in = blobStore.blobStreamForKey(blobKey);
        if (in == null) {
            Log.w(Log.TAG_SYNC, "Unable to load the blob stream for blobKey: " + blobKey);
            throw new IOException("Unable to load the blob stream for blobKey: " + blobKey);
        }
        try {
            byte[] tmp = new byte[4096];
            int l;
            while ((l = in.read(tmp)) != -1)
                out.write(tmp, 0, l);
            out.flush();
        } finally {
            in.close();
        }
    }

    public String getTransferEncoding() {
        return MIME.ENC_BINARY;
    }

    public String getCharset() {
        return null;
    }

    public long getContentLength() {
        return contentLength;
    }

    public String getFilename() {
        return this.filename;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }
}
