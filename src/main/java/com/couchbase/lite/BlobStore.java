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

import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.support.action.Action;
import com.couchbase.lite.support.action.ActionBlock;
import com.couchbase.lite.support.action.ActionException;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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

    private Context context;
    private String path;
    private SymmetricKey encryptionKey;
    private BlobStore tempStore;

    public BlobStore(Context context, String path, SymmetricKey encryptionKey) throws CouchbaseLiteException {
        this(context, path, encryptionKey, false);
    }

    public BlobStore(Context context, String path, SymmetricKey encryptionKey, boolean autoMigrate)
            throws CouchbaseLiteException {
        this.context = context;
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
                        encryptionAlg + '\'');
                throw new CouchbaseLiteException(Status.UNAUTHORIZED);
            }
        } else if (!isMarkerExists) {
            // No "_encryption" file was found, so on-disk store isn't encrypted:
            SymmetricKey encKey = encryptionKey;
            if (encKey != null) {
                // In general, this case shouldn't happen but just in case:
                Log.i(Log.TAG_DATABASE, "BlobStore: BlobStore should be encrypted; do it now...");
                encryptionKey = null;
                try {
                    changeEncryptionKey(encKey);
                } catch (ActionException e) {
                    throw new CouchbaseLiteException("Cannot change the attachment encryption key", e,
                            Status.ATTACHMENT_ERROR);
                }
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

    public Action actionToChangeEncryptionKey(final SymmetricKey newKey) {
        Action action = new Action();

        // Backup oldKey:
        final SymmetricKey oldKey = encryptionKey;

        // Find all blob files:
        File directory = new File(path);
        File[] files = null;
        if (directory.exists() && directory.isDirectory()) {
            files = directory.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(FILE_EXTENSION);
                }
            });
        }
        // Make final:
        final File[] blobs = files;

        if (blobs == null || blobs.length == 0) {
            // No blobs, so nothing to encrypt. Just add/remove the encryption marker file:
            action.add(
                // Perform:
                new ActionBlock() {
                    @Override
                    public void execute() throws ActionException {
                        Log.i(Log.TAG_DATABASE, "BlobStore: " +
                                (newKey != null ? "encrypting" : "decrypting") + ' ' + path);
                        Log.i(Log.TAG_DATABASE, "BlobStore: **No blobs to copy; done.**");
                        encryptionKey = newKey;
                        try {
                            markEncrypted(newKey != null);
                        } catch (CouchbaseLiteException e) {
                            throw new ActionException(e);
                        }
                    }
                },
                // Backout:
                new ActionBlock() {
                    @Override
                    public void execute() throws ActionException {
                        encryptionKey = oldKey;
                    }
                }, null
            );

            return action;
        }

        // Create a new directory for the new blob store. Have to do this now, before starting the
        // action, because farther down we create an action to move it...
        File tempDir = new File (context.getTempDir(), Misc.CreateUUID());
        final File tempStoreDir = tempDir.mkdirs() ? tempDir : null;
        // Mark delete on exit (optional and work on Java only):
        if (tempStoreDir != null)
            tempStoreDir.deleteOnExit();

        action.add(
            // Perform:
            new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    Log.i(Log.TAG_DATABASE, "BlobStore: " +
                            (newKey != null ? "encrypting" : "decrypting") + ' ' + path);
                    if (tempStoreDir == null)
                        throw  new ActionException("Cannot create a temporary directory");
                }
            },
            // Backout:
            new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    if (!FileDirUtils.deleteRecursive(tempStoreDir))
                        throw new ActionException("Cannot delete a temporary directory " +
                                tempStoreDir.getAbsolutePath());
                }
            }, null
        );

        // Create a new temporary BlobStore
        action.add(new ActionBlock() {
            @Override
            public void execute() throws ActionException {
                try {
                    tempStore = new BlobStore(context, tempStoreDir.getAbsolutePath(), newKey);
                    tempStore.markEncrypted(newKey != null);
                } catch (CouchbaseLiteException e) {
                    throw  new ActionException(e);
                }
            }
        }, null, null);

        // Copy each of my blobs into the new store (which will update its encryption):
        action.add(new ActionBlock() {
            @Override
            public void execute() throws ActionException {
                for (File blob : blobs) {
                    InputStream readStream = null;
                    BlobStoreWriter writer = null;
                    try {
                        Log.i(Log.TAG_DATABASE, "BlobStore: Copying " + blob);
                        readStream = encryptionKey.decryptStream(new FileInputStream(blob));
                        writer = new BlobStoreWriter(tempStore);
                        writer.appendInputStream(readStream);
                        writer.finish();
                        writer.install();
                    } catch (Exception e) {
                        if (writer != null)
                            writer.cancel();
                        throw new ActionException(e);
                    } finally {
                        // Mark delete on exit (optional and work on Java only):
                        if (writer != null)
                            new File(tempStore.getRawPathForKey(writer.getBlobKey())).deleteOnExit();
                        // Close readStream:
                        try {
                            if (readStream != null)
                                readStream.close();
                        } catch (IOException e) { }
                    }
                }
            }
        }, null, null);

        // Replace the attachment dir with the new one:
        action.add(Action.moveAndReplaceFile(tempStoreDir.getAbsolutePath(), path,
                context.getTempDir().getAbsolutePath()));

        // Finally update encryptionKey:
        action.add(
            new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    encryptionKey = newKey;
                }
            }, new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    encryptionKey = oldKey;
                }
            }, null
        );

        return action;
    }

    public void changeEncryptionKey(SymmetricKey newKey) throws ActionException {
        actionToChangeEncryptionKey(newKey).run();
    }

    protected static void migrateBlobstoreFilenames(File directory) {
        if (directory == null || !directory.isDirectory())
            return;

        File[] files = directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(FILE_EXTENSION);
            }
        });

        for (File file : files) {
            String name = file.getName();
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
        } catch (OutOfMemoryError e) {
            blob = null;
            Log.e(Log.TAG_DATABASE, "BlobStore: Error reading file", e);
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

    public boolean storeBlob(byte[] data, BlobKey outKey) {
        BlobKey newKey = keyForBlob(data);
        outKey.setBytes(newKey.getBytes());
        String path = getRawPathForKey(outKey);
        File file = new File(path);
        if (file.canRead()) {
            return true;
        }

        if (encryptionKey != null) {
            try {
                data = encryptionKey.encryptData(data);
            } catch (SymmetricKeyException e) {
                Log.w(Log.TAG_DATABASE, "BlobStore: Failed to encode data for " + path, e);
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
        InputStream is = null;
        try {
            is = new FileInputStream(file);

            // Get the size of the file:
            long length = file.length();
            if (length > (long)Integer.MAX_VALUE)
                throw new OutOfMemoryError("The file is too large to read into a byte array.");

            // Create the byte array to hold the data:
            byte[] bytes = new byte[(int) length];
            if (bytes == null)
                throw new OutOfMemoryError("The file is too large to read into a byte array.");

            // Read in the bytes:
            int offset = 0;
            int numRead = 0;
            while (offset < bytes.length
                    && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
                offset += numRead;
            }

            // Ensure all the bytes have been read in
            if (offset < bytes.length)
                throw new IOException("Could not completely read file " + file.getName());
            return bytes;
        } finally {
             if (is != null)
                 is.close();
        }
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
                        Log.e(Log.TAG_DATABASE, "BlobStore: Error deleting attachment: %s", attachment);
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
                Log.e(Log.TAG_BLOB_STORE, "Failed to read from RandomAccessFile", e);
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
            throw new IllegalStateException(String.format(Locale.ENGLISH, "Unable to create directory for: %s", tempDirectory));
        }

        return tempDirectory;
    }

    public String getPath() {
        return path;
    }

    public SymmetricKey getEncryptionKey() {
        return encryptionKey;
    }

    public boolean isEncrypted() {
        return encryptionKey != null;
    }
}
