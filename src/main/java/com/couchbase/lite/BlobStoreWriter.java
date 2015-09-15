package com.couchbase.lite;

import com.couchbase.lite.support.Base64;
import com.couchbase.lite.support.security.SymmetricKey;
import com.couchbase.lite.support.security.SymmetricKeyException;
import com.couchbase.lite.util.Log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Lets you stream a large attachment to a BlobStore asynchronously, e.g. from a network download.
 *
 * @exclude
 */
public class BlobStoreWriter {

    /**
     * The underlying blob store where it should be stored.
     */
    private BlobStore store = null;

    /**
     * The number of bytes in the blob.
     */
    private int length = 0;

    /**
     * After finishing, this is the key for looking up the blob through the CBL_BlobStore.
     */
    private BlobKey blobKey = null;

    /**
     * After finishing, store md5 getDigest result here
     */
    private byte[] md5DigestResult = null;

    /**
     * Message getDigest for sha1 that is updated as data is appended
     */
    private MessageDigest sha1Digest = null;
    private MessageDigest md5Digest = null;

    private BufferedOutputStream outStream = null;
    private File tempFile = null;

    /**
     * An encryptor for encrypting the blob content.
     */
    private SymmetricKey.Encryptor encryptor = null;

    public BlobStoreWriter(BlobStore store) {
        this.store = store;
        try {
            sha1Digest = MessageDigest.getInstance("SHA-1");
            sha1Digest.reset();
            md5Digest = MessageDigest.getInstance("MD5");
            md5Digest.reset();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try {
            openTempFile();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        SymmetricKey encryptionKey = store.getEncryptionKey();
        if (encryptionKey != null) {
            try {
                encryptor = encryptionKey.createEncryptor();
            } catch (SymmetricKeyException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private void openTempFile() throws IOException {
        File tempDir = store.tempDir();
        String filePrefix = BlobStore.TMP_FILE_PREFIX;
        String fileExtension = BlobStore.TMP_FILE_EXTENSION;
        tempFile = File.createTempFile(filePrefix, fileExtension, tempDir);
        outStream = new BufferedOutputStream(new FileOutputStream(tempFile));
    }

    /**
     * Appends data to the blob. Call this when new data is available.
     */
    public void appendData(byte[] data) throws IOException, SymmetricKeyException {
        if (data == null)
            return;
        appendData(data, 0, data.length);
    }

    public void appendData(byte[] data, int off, int len) throws IOException, SymmetricKeyException {
        if (data == null)
            return;
        length += len;
        sha1Digest.update(data, off, len);
        md5Digest.update(data, off, len);
        if (encryptor != null) {
            data = encryptor.encrypt(data, off, len);
            if (data != null)
                outStream.write(data, 0, data.length);
        } else
            outStream.write(data, off, len);
    }

    void appendInputStream(InputStream inputStream) throws IOException, SymmetricKeyException {
        byte[] buffer = new byte[1024];
        int len;
        length = 0;
        try {
            while ((len = inputStream.read(buffer)) != -1) {
                appendData(buffer, 0, len);
            }
        } finally {
            try {
                // Question: Should this method close the stream?
                if (inputStream != null)
                    inputStream.close();
            } catch (IOException e) {
                Log.w(Log.TAG_BLOB_STORE, "Exception closing input stream", e);
            }
        }
    }

    /**
     * Call this after all the data has been added.
     */
    public void finish() throws IOException, SymmetricKeyException {
        if (outStream != null) {
            if (encryptor != null)
                outStream.write(encryptor.encrypt(null));

            // FileOutputStream is also closed cascadingly
            outStream.close();
            outStream = null;

            // Only create the key if we got all the data successfully
            blobKey = new BlobKey(sha1Digest.digest());
            md5DigestResult = md5Digest.digest();
        }
    }

    /**
     * Call this to cancel before finishing the data.
     */
    public void cancel() {
        try {
            // FileOutputStream is also closed cascadingly
            if (outStream != null) {
                outStream.close();
                outStream = null;
            }
            // Clear encryptor:
            encryptor = null;
        } catch (IOException e) {
            Log.w(Log.TAG_BLOB_STORE, "Exception closing buffered output stream", e);
        }
        tempFile.delete();
    }

    /**
     * Installs a finished blob into the store.
     */
    public boolean install() {
        if (tempFile == null)
            return true;  // already installed
        // Move temp file to correct location in blob store:
        String destPath = store.getRawPathForKey(blobKey);
        File destPathFile = new File(destPath);
        if (tempFile.renameTo(destPathFile))
            // If the move fails, assume it means a file with the same name already exists; in that
            // case it must have the identical contents, so we're still OK.
            tempFile = null;
        else
            cancel();
        return true;
    }

    public String mD5DigestString() {
        String base64Md5Digest = Base64.encodeBytes(md5DigestResult);
        return String.format("md5-%s", base64Md5Digest);
    }

    public String sHA1DigestString() {
        String base64Sha1Digest = Base64.encodeBytes(blobKey.getBytes());
        return String.format("sha1-%s", base64Sha1Digest);
    }

    public int getLength() {
        return length;
    }

    public BlobKey getBlobKey() {
        return blobKey;
    }

    public String getFilePath() {
        return tempFile.getPath();
    }
}
