//
// Copyright (c) 2016 Couchbase, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
//
package com.couchbase.lite.replicator;

import com.couchbase.lite.BlobKey;
import com.couchbase.lite.BlobStore;

import java.io.IOException;
import java.io.InputStream;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

/**
 * Created by hideki on 5/16/16.
 */
public class BlobRequestBody {
    public static RequestBody create(final MediaType contentType,
                                     final BlobStore blobStore,
                                     final BlobKey blobKey,
                                     final long declaredLength,
                                     final boolean syncGateway) {
        if (blobStore == null) throw new NullPointerException("blobStore == null");
        if (blobKey == null) throw new NullPointerException("blobKey == null");

        return new RequestBody() {
            @Override
            public MediaType contentType() {
                return contentType;
            }


            /**
             * OkHttp use chunked transfer if content-length is unknown. And CouchDB 1.6.1 can not
             * handle chunked transer with multipart doc, then CouchDB disconnect .
             * To send non-chunked transfer with multipart document by OkHttp, the client needs
             * to specify content-length.
             * Sync Gateway can accept chunked transfer
             */
            @Override
            public long contentLength() throws IOException {
                // Sync Gateway
                if (syncGateway) {
                    return super.contentLength();
                }
                // CouchDB or Couchbase Lite
                else {
                    // encrypted
                    if (blobStore.isEncrypted()) {
                        if (declaredLength > 0)
                            return declaredLength;
                        else
                            return super.contentLength();
                    } else {
                        // gzipped or non-encrypted content
                        // NOTE: gzipped: CBL Java sends gzipped contents without decode.
                        return blobStore.getSizeOfBlob(blobKey);
                    }
                }
            }

            @Override
            public void writeTo(BufferedSink sink) throws IOException {
                InputStream in = blobStore.blobStreamForKey(blobKey);
                if (in == null)
                    throw new IOException("Unable to load the blob stream for blobKey: " + blobKey);
                Source source = null;
                try {
                    source = Okio.source(in);
                    sink.writeAll(source);
                } finally {
                    // close InputStream
                    Util.closeQuietly(source);
                }
            }
        };
    }
}
