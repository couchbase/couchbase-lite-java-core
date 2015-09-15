package com.couchbase.lite.support;

import com.couchbase.lite.BlobStoreWriter;
import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Misc;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import org.apache.http.util.ByteArrayBuffer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class MultipartDocumentReader implements MultipartReaderDelegate {

    private MultipartReader multipartReader;
    private BlobStoreWriter curAttachment;
    private ByteArrayBuffer jsonBuffer;
    private boolean jsonCompressed;
    private Map<String, Object> document;
    private Database database;
    private Map<String, BlobStoreWriter> attachmentsByName;
    private Map<String, BlobStoreWriter> attachmentsByMd5Digest;

    public MultipartDocumentReader(Database database) {
        this.database = database;
    }

    public Map<String, Object> getDocumentProperties() {
        return document;
    }

    public void parseJsonBuffer() {
        ByteArrayInputStream inputStream = null;
        try {
            inputStream = new ByteArrayInputStream(jsonBuffer.buffer(), 0, jsonBuffer.length());
            // compressed json
            if (jsonCompressed) {
                GZIPInputStream gzipStream = null;
                try {
                    gzipStream = new GZIPInputStream(inputStream);
                    document = Manager.getObjectMapper().readValue(gzipStream, Map.class);
                } finally {
                    if (gzipStream != null) {
                        try {
                            gzipStream.close();
                        } catch (IOException e) {
                        }
                    }
                }
            }
            //  plain json
            else {
                document = Manager.getObjectMapper().readValue(inputStream, Map.class);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse json buffer", e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                }
            }
            jsonBuffer.clear();
            jsonBuffer = null;
        }
    }

    public void setHeaders(Map<String, String> headers) {
        String contentType = headers.get("Content-Type");
        if (contentType.startsWith("multipart/")) {
            // Multipart, so initialize the parser:
            multipartReader = new MultipartReader(contentType, this);
            attachmentsByName = new HashMap<String, BlobStoreWriter>();
            attachmentsByMd5Digest = new HashMap<String, BlobStoreWriter>();
        } else if (contentType == null ||
                contentType.startsWith("application/json") ||
                contentType.startsWith("text/plain")) {

            // No multipart, so no attachments. Body is pure JSON. (We allow text/plain because CouchDB
            // sends JSON responses using the wrong content-type.)
            startJSONBufferWithHeaders(headers);
        } else {
            throw new IllegalArgumentException("Unknown/invalid MIME type");
        }
    }

    protected void startJSONBufferWithHeaders(Map<String, String> headers) {
        jsonBuffer = new ByteArrayBuffer(1024);
        jsonCompressed = Utils.isGzip(headers.get("Content-Encoding"));
    }

    public void appendData(byte[] data) {
        if (multipartReader != null) {
            multipartReader.appendData(data);
        } else {
            jsonBuffer.append(data, 0, data.length);
        }
    }

    public void appendData(byte[] data, int off, int len) {
        if (multipartReader != null) {
            multipartReader.appendData(data, off, len);
        } else {
            jsonBuffer.append(data, off, len);
        }
    }

    public void finish() {
        if (multipartReader != null) {
            if (!multipartReader.finished()) {
                throw new IllegalStateException("received incomplete MIME multipart response");
            }

            registerAttachments();
        } else {
            parseJsonBuffer();
        }
    }

    private void registerAttachments() {

        int numAttachmentsInDoc = 0;

        Map<String, Object> attachments = (Map<String, Object>) document.get("_attachments");
        if (attachments == null) {
            return;
        }

        for (String attachmentName : attachments.keySet()) {
            Map<String, Object> attachment = (Map<String, Object>) attachments.get(attachmentName);

            int length = 0;
            if (attachment.containsKey("length")) {
                length = ((Integer) attachment.get("length")).intValue();
            }
            if (attachment.containsKey("encoded_length")) {
                length = ((Integer) attachment.get("encoded_length")).intValue();
            }

            if (attachment.containsKey("follows") &&
                    ((Boolean) attachment.get("follows")).booleanValue() == true) {

                // Check that each attachment in the JSON corresponds to an attachment MIME body.
                // Look up the attachment by either its MIME Content-Disposition header or MD5 getDigest:
                String digest = (String) attachment.get("digest");
                BlobStoreWriter writer = attachmentsByName.get(attachmentName);
                if (writer != null) {
                    // Identified the MIME body by the filename in its Disposition header:
                    String actualDigest = writer.mD5DigestString();
                    if (digest != null &&
                            !digest.equals(actualDigest) &&
                            !digest.equals(writer.sHA1DigestString())) {
                        String errMsg = String.format("Attachment '%s' has incorrect MD5 getDigest (%s; should be either %s or %s)",
                                attachmentName, digest, actualDigest, writer.sHA1DigestString());
                        throw new IllegalStateException(errMsg);
                    }
                    attachment.put("digest", actualDigest);

                } else if (digest != null) {
                    writer = attachmentsByMd5Digest.get(digest);
                    if (writer == null) {
                        String errMsg = String.format("Attachment '%s' does not appear in MIME body",
                                attachmentName);
                        throw new IllegalStateException(errMsg);
                    }
                } else if (attachments.size() == 1 && attachmentsByMd5Digest.size() == 1) {
                    // Else there's only one attachment, so just assume it matches & use it:
                    writer = attachmentsByMd5Digest.values().iterator().next();
                    attachment.put("digest", writer.mD5DigestString());
                } else {
                    // No getDigest metatata, no filename in MIME body; give up:
                    String errMsg = String.format("Attachment '%s' has no getDigest metadata; cannot identify MIME body",
                            attachmentName);
                    throw new IllegalStateException(errMsg);
                }

                // Check that the length matches:
                if (writer.getLength() != length) {
                    String errMsg = String.format("Attachment '%s' has incorrect length field %d (should be %d)",
                            attachmentName, length, writer.getLength());
                    throw new IllegalStateException(errMsg);
                }

                ++numAttachmentsInDoc;

            } else if (attachment.containsKey("data") && length > 1000) {
                Log.w(Log.TAG_REMOTE_REQUEST, "Attachment '%s' sent inline (len=%d).  Large attachments " +
                        "should be sent in MIME parts for reduced memory overhead.", attachmentName, length);
            }
        }

        if (numAttachmentsInDoc < attachmentsByMd5Digest.size()) {
            String msg = String.format("More MIME bodies (%d) than attachments (%d) ",
                    attachmentsByMd5Digest.size(), numAttachmentsInDoc);
            throw new IllegalStateException(msg);
        }

        // hand over the (uninstalled) blobs to the database to remember:
        database.rememberAttachmentWritersForDigests(attachmentsByMd5Digest);

    }

    @Override
    public void startedPart(Map<String, String> headers) {

        if (document == null) {
            startJSONBufferWithHeaders(headers);
        } else {
            curAttachment = database.getAttachmentWriter();

            String contentDisposition = headers.get("Content-Disposition");
            if (contentDisposition != null && contentDisposition.startsWith("attachment; filename=")) {
                // TODO: Parse this less simplistically. Right now it assumes it's in exactly the same
                // format generated by -[CBL_Pusher uploadMultipartRevision:]. CouchDB (as of 1.2) doesn't
                // output any headers at all on attachments so there's no compatibility issue yet.

                String contentDispositionUnquoted = Misc.unquoteString(contentDisposition);
                String name = contentDispositionUnquoted.substring(21);

                if (name != null) {
                    attachmentsByName.put(name, curAttachment);
                }
            }
        }
    }

    @Override
    public void appendToPart(byte[] data) {
        appendToPart(data, 0, data.length);
    }

    @Override
    public void appendToPart(final byte[] data, int off, int len) {
        if (jsonBuffer != null) {
            jsonBuffer.append(data, off, len);
        } else {
            try {
                curAttachment.appendData(data, off, len);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to append data", e);
            }
        }
    }

    @Override
    public void finishedPart() {
        if (jsonBuffer != null) {
            parseJsonBuffer();
        } else {
            try {
                curAttachment.finish();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to finish attachment", e);
            }
            String md5String = curAttachment.mD5DigestString();
            attachmentsByMd5Digest.put(md5String, curAttachment);
            curAttachment = null;
        }
    }
}
