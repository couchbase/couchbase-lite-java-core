/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 * <p/>
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
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

package com.couchbase.lite;

import com.couchbase.lite.support.security.SymmetricKey;
import com.couchbase.lite.support.security.SymmetricKeyException;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.TextUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * A persistent content-addressable store for arbitrary-size data blobs.
 * Each blob is stored as a file named by its SHA-1 getDigest.
 * @exclude
 */
public class BlobStore {
    public static final String FILE_EXTENSION = ".blob";
    public static final String TMP_FILE_EXTENSION = ".blobtmp";
    public static final String TMP_FILE_PREFIX = "tmp";
    public static final String ENCRYPTION_ALGORITHM = "AES";
    public static final String ENCRYPTION_MARKER_FILENAME = "_encryption";

    private String path;
    private SymmetricKey encryptionKey;

    public BlobStore(String path, SymmetricKey encryptionKey) throws CouchbaseLiteException {
        this(path, encryptionKey, false);
    }

    public BlobStore(String path, SymmetricKey encryptionKey, boolean autoMigrate)
            throws CouchbaseLiteException {
        this.path = path;
        this.encryptionKey = encryptionKey;

        File directory = new File(path);
        if (directory.exists()) {
            if (!directory.isDirectory())
                throw new CouchbaseLiteException("BlobStore: Blobstore is not a directory", Status.ATTACHMENT_ERROR);
            verifyExistingStore();
        } else {
            if (!directory.mkdirs()) {
                Log.w(Log.TAG_DATABASE, "BlobStore: Unable to make directory: %s", directory);
                throw new CouchbaseLiteException("Unable to create a blobstore", Status.ATTACHMENT_ERROR);
            }
            if (encryptionKey != null)
                markEncrypted(true);
        }

        // migrate blobstore filenames.
        if (autoMigrate) {
            migrateBlobstoreFilenames(directory);
        }
    }

    private void verifyExistingStore() throws CouchbaseLiteException {
        File markerFile = new File(path, ENCRYPTION_MARKER_FILENAME);
        boolean isMarkerExists = markerFile.exists();
        String encryptionAlg = null;
        if (isMarkerExists) {
            try {
                encryptionAlg = TextUtils.read(markerFile);
            } catch (IOException e) {
                throw new CouchbaseLiteException(e.getCause(), Status.BAD_ATTACHMENT);
            }
        }

        if (encryptionAlg != null) {
            // "_encryption" file is present, so make sure we support its format & have a key:
            if (encryptionKey == null) {
                Log.w(Log.TAG_DATABASE, "BlobStore: Opening encrypted blob-store without providing a key");
                throw new CouchbaseLiteException(Status.UNAUTHORIZED);
            } else if (!encryptionAlg.equals(ENCRYPTION_ALGORITHM)) {
                Log.w(Log.TAG_DATABASE, "BlobStore: Blob store uses unrecognized encryption '" +
                        encryptionAlg + "'");
                throw new CouchbaseLiteException(Status.UNAUTHORIZED);
            }
        } else if (!isMarkerExists) {
            // No "_encryption" file was found, so on-disk store isn't encrypted:
            if (encryptionKey != null) {
                // This shouldn't be happening:
                Log.e(Log.TAG_DATABASE, "BlobStore: Blob store was corrupted. Encryption marker file was not found");
                throw new CouchbaseLiteException(Status.CORRUPT_ERROR);
            }
        }
    }

    private void markEncrypted(boolean encrypted) throws CouchbaseLiteException {
        File markerFile = new File(path, ENCRYPTION_MARKER_FILENAME);
        if (encrypted) {
            try {
                TextUtils.write(ENCRYPTION_ALGORITHM, markerFile);
                if (!markerFile.exists()) {
                    Log.w(Log.TAG_DATABASE, "BlobStore: Unable to save the encryption marker file into the blob store");
                }
            } catch (IOException e) {
                Log.w(Log.TAG_DATABASE, "BlobStore: Unable to save the encryption marker file into the blob store");
                throw new CouchbaseLiteException(e.getCause(), Status.ATTACHMENT_ERROR);
            }
        } else {
            if (markerFile.exists()) {
                if (!markerFile.delete()) {
                    Log.w(Log.TAG_DATABASE, "BlobStore: Unable to delete the encryption marker file in the blob store");
                    throw new CouchbaseLiteException(
                            "Unable to delete the encryption marker file in the Blob-store",
                            Status.ATTACHMENT_ERROR);
                }
            }
        }
    }

    protected void migrateBlobstoreFilenames(File directory) {
        if (directory == null || !directory.isDirectory())
            return;

        File[] files = directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(FILE_EXTENSION);
            }
        });

        for (File file : files) {
            String name = file.getName().substring(0);
            name = name.substring(0, name.indexOf(FILE_EXTENSION));
            File dest = new File(directory, name.toUpperCase() + FILE_EXTENSION);
            file.renameTo(dest);
        }
    }

    public static BlobKey keyForBlob(byte[] data) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Log.e(Log.TAG_DATABASE, "BlobStore: Error, SHA-1 getDigest is unavailable.");
            return null;
        }
        byte[] sha1hash = new byte[40];
        md.update(data, 0, data.length);
        sha1hash = md.digest();
        BlobKey result = new BlobKey(sha1hash);
        return result;
    }

    public static BlobKey keyForBlobFromFile(File file) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Log.e(Log.TAG_DATABASE, "BlobStore: Error, SHA-1 getDigest is unavailable.");
            return null;
        }
        byte[] sha1hash = new byte[40];

        try {
            FileInputStream fis = new FileInputStream(file);
            byte[] buffer = new byte[65536];
            int lenRead = fis.read(buffer);
            while (lenRead > 0) {
                md.update(buffer, 0, lenRead);
                lenRead = fis.read(buffer);
            }
            fis.close();
        } catch (IOException e) {
            Log.e(Log.TAG_DATABASE, "BlobStore: Error reading tmp file to compute key");
        }

        sha1hash = md.digest();
        BlobKey result = new BlobKey(sha1hash);
        return result;
    }

    /**
     * Path to file storing the blob. Returns null if the blob is encrypted.
     * @param key
     * @return path to file storing the blob.
     */
    public String getBlobPathForKey(BlobKey key) {
        if (encryptionKey != null)
            return null;
        return getRawPathForKey(key);
    }

    public String getRawPathForKey(BlobKey key) {
        String hexKey = BlobKey.convertToHex(key.getBytes());

        String filename = path + File.separator + hexKey + FILE_EXTENSION;

        // CBL Android/Java used to use lowercase hex string. But it was inconsistent with CBL iOS.
        // We fixed it in BlobKey.covertToHex().
        // In case user will migrate from older version of CBL to newer version, it causes filename
        // mismatch by upper or lower case.
        // As getRawPathForKey() method is called from many other functions, file mismatch check is
        // added here.
        File file = new File(filename);
        if (!file.exists()) {
            String lowercaseFilename = path + File.separator + hexKey.toLowerCase() + FILE_EXTENSION;
            File lowercaseFile = new File(lowercaseFilename);
            if (lowercaseFile.exists()) {
                filename = lowercaseFilename;
                Log.w(Log.TAG_DATABASE,
                        "BlobStore: Found the older attachment blobstore file. Recommend to set auto migration:\n" +
                                "\tManagerOptions options = new ManagerOptions();\n" +
                                "\toptions.setAutoMigrateBlobStoreFilename(true);\n" +
                                "\tManager manager = new Manager(..., options);\n"
                );
            }
        }

        return filename;
    }

    public long getSizeOfBlob(BlobKey key) {
        String path = getRawPathForKey(key);
        File file = new File(path);
        return file.length();
    }

    public boolean getKeyForFilename(BlobKey outKey, String filename) {
        if (!filename.endsWith(FILE_EXTENSION)) {
            return false;
        }
        //trim off extension
        String rest = filename.substring(path.length() + 1, filename.length() - FILE_EXTENSION.length());

        outKey.setBytes(BlobKey.convertFromHex(rest));

        return true;
    }

    public boolean hasBlobForKey(BlobKey key) {
        if(key == null) return false;
        String path = getRawPathForKey(key);
        File file = new File(path);
        return file.isFile() && file.exists();
    }

    public byte[] blobForKey(BlobKey key) {
        if (key == null)
            return null;

        String path = getRawPathForKey(key);
        File file = new File(path);
        byte[] blob = null;
        try {
            blob = getBytesFromFile(file);
            if (encryptionKey != null && blob != null)
                blob = encryptionKey.decryptData(blob);
            BlobKey decodedKey = BlobStore.keyForBlob(blob);
            if (!key.equals(decodedKey)) {
                Log.w(Log.TAG_DATABASE, "BlobStore: Attachment " + path + " decoded incorrectly!");
                blob = null;
            }
        } catch (IOException e) {
            blob = null;
            Log.e(Log.TAG_DATABASE, "BlobStore: Error reading file", e);
        } catch (SymmetricKeyException e) {
            blob = null;
            Log.e(Log.TAG_DATABASE, "BlobStore: Attachment " + path + " decoded incorrectly!", e);
        }
        return blob;
    }

    public InputStream blobStreamForKey(BlobKey key) {
        String path = getRawPathForKey(key);
        File file = new File(path);
        if (file.canRead()) {
            try {
                InputStream is = new FileInputStream(file);
                if (encryptionKey != null)
                    return encryptionKey.decryptStream(is);
                else
                    return is;
            } catch (FileNotFoundException e) {
                Log.e(Log.TAG_DATABASE, "BlobStore: Unexpected file not found in blob store", e);
                return null;
            } catch (SymmetricKeyException e) {
                Log.e(Log.TAG_DATABASE, "BlobStore: Attachment stream " + path + " cannot be decoded!", e);
                return null;
            }
        }
        return null;
    }

    /*
    NO Usages: Should be removed:
    public boolean storeBlobStream(InputStream inputStream, BlobKey outKey) {
        File tmp = null;
        try {
            tmp = File.createTempFile(TMP_FILE_PREFIX, TMP_FILE_EXTENSION, new File(path));
            FileOutputStream fos = new FileOutputStream(tmp);
            byte[] buffer = new byte[65536];
            int lenRead = inputStream.read(buffer);
            while (lenRead > 0) {
                fos.write(buffer, 0, lenRead);
                lenRead = inputStream.read(buffer);
            }
            inputStream.close();
            fos.close();
        } catch (IOException e) {
            Log.e(Log.TAG_DATABASE, "BlobStore: Error writing blob to tmp file", e);
            return false;
        }

        BlobKey newKey = keyForBlobFromFile(tmp);
        outKey.setBytes(newKey.getBytes());
        String path = getRawPathForKey(outKey);
        File file = new File(path);

        if (file.canRead()) {
            // object with this hash already exists, we should delete tmp file and return true
            tmp.delete();
            return true;
        } else {
            // does not exist, we should rename tmp file to this name
            tmp.renameTo(file);
        }
        return true;
    }
    */

    public boolean storeBlob(byte[] data, BlobKey outKey) {
        BlobKey newKey = keyForBlob(data);
        outKey.setBytes(newKey.getBytes());
        String path = getRawPathForKey(outKey);
        File file = new File(path);
        if (file.canRead()) {
            return true;
        }

        if (encryptionKey != null) {
            SymmetricKeyException error = null;
            try {
                data = encryptionKey.encryptData(data);
            } catch (SymmetricKeyException e) {
                error = e;
            }
            if (data == null) {
                Log.w(Log.TAG_DATABASE, "BlobStore: Failed to encode data for " + path, error);
                return false;
            }
        }

        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            fos.write(data);
        } catch (FileNotFoundException e) {
            Log.e(Log.TAG_DATABASE, "BlobStore: Error opening file for output", e);
            return false;
        } catch (IOException ioe) {
            Log.e(Log.TAG_DATABASE, "BlobStore: Error writing to file", ioe);
            return false;
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        return true;
    }

    private static byte[] getBytesFromFile(File file) throws IOException {
        InputStream is = new FileInputStream(file);

        // Get the size of the file
        long length = file.length();

        // Create the byte array to hold the data
        byte[] bytes = new byte[(int) length];

        // Read in the bytes
        int offset = 0;
        int numRead = 0;
        while (offset < bytes.length
                && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
            offset += numRead;
        }

        // Ensure all the bytes have been read in
        if (offset < bytes.length) {
            throw new IOException("Could not completely read file " + file.getName());
        }

        // Close the input stream and return bytes
        is.close();
        return bytes;
    }

    public Set<BlobKey> allKeys() {
        Set<BlobKey> result = new HashSet<BlobKey>();
        File file = new File(path);
        File[] contents = file.listFiles();
        for (File attachment : contents) {
            if (attachment.isDirectory()) {
                continue;
            }
            BlobKey attachmentKey = new BlobKey();
            getKeyForFilename(attachmentKey, attachment.getPath());
            result.add(attachmentKey);
        }
        return result;
    }

    public int count() {
        File file = new File(path);
        File[] contents = file.listFiles();
        return contents.length;
    }

    public long totalDataSize() {
        long total = 0;
        File file = new File(path);
        File[] contents = file.listFiles();
        for (File attachment : contents) {
            total += attachment.length();
        }
        return total;
    }

    public int deleteBlobsExceptWithKeys(List<BlobKey> keysToKeep) {
        int numDeleted = 0;
        File file = new File(path);
        File[] contents = file.listFiles();
        for (File attachment : contents) {
            BlobKey attachmentKey = new BlobKey();
            if (getKeyForFilename(attachmentKey, attachment.getPath())) {
                if (!keysToKeep.contains(attachmentKey)) {
                    boolean result = attachment.delete();
                    if (result) {
                        ++numDeleted;
                    } else {
                        Log.e(Log.TAG_BLOB_STORE, "Error deleting attachment: %s", attachment);
                    }
                }
            }
        }
        return numDeleted;
    }

    public int deleteBlobs() {
        return deleteBlobsExceptWithKeys(new ArrayList<BlobKey>());
    }

    public boolean isGZipped(BlobKey key) {
        int magic = 0;
        String path = getRawPathForKey(key);
        File file = new File(path);
        if (file.canRead()) {
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                magic = raf.read() & 0xff | ((raf.read() << 8) & 0xff00);
                raf.close();
            } catch (Throwable e) {
                e.printStackTrace(System.err);
            }
        }
        return magic == GZIPInputStream.GZIP_MAGIC;
    }

    public File tempDir() {
        File directory = new File(path);
        File tempDirectory = new File(directory, "temp_attachments");

        if (!tempDirectory.exists()) {
            tempDirectory.mkdirs();
        }
        if (!tempDirectory.isDirectory()) {
            throw new IllegalStateException(String.format("Unable to create directory for: %s", tempDirectory));
        }

        return tempDirectory;
    }

    public String getPath() {
        return path;
    }

    public SymmetricKey getEncryptionKey() {
        return encryptionKey;
    }
}
