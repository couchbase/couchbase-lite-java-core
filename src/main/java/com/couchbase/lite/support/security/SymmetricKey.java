/**
 * Created by Pasin Suriyentrakorn on 8/27/15.
 * <p/>
 * Copyright (c) 2015 Couchbase, Inc All rights reserved.
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
 *
 */

package com.couchbase.lite.support.security;

import com.couchbase.lite.util.ArrayUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class SymmetricKey {
    // Key:
    public static final int KEY_SIZE = 32; // AES256

    // Encryption:
    private static final int BLOCK_SIZE = 16; // AES use a 128-bit block
    private static final int IV_SIZE = BLOCK_SIZE;

    // Raw key data:
    private byte[] keyData = null;

    // Remember when using BC provider:
    private boolean useBCProvider = false;

    /**
     * Create an instance with a secure random generated key.
     * @throws SymmetricKeyException
     */
    public SymmetricKey() throws SymmetricKeyException {
        this(generateKey(KEY_SIZE));
    }

    /**
     * Create an instance with a raw key. The size of the key needs to be 32 bytes.
     * @param key
     * @throws SymmetricKeyException
     */
    public SymmetricKey(byte[] key) throws SymmetricKeyException {
        initWithKey(key);
    }

    /**
     * Initialize the object with a raw key of 32 bytes size.
     * @param key 32 bytes raw key
     * @throws SymmetricKeyException
     */
    private void initWithKey(byte[] key) throws SymmetricKeyException {
        if (key == null)
            throw new SymmetricKeyException("Key cannot be null");
        if (key.length != KEY_SIZE)
            throw new SymmetricKeyException("Key size is not " + KEY_SIZE + "bytes");
        keyData = key;
    }

    /**
     * Get the raw key data derived from a password or the raw key given to
     * SymmetricKey(byte[] key) constructor.
     * @return raw key data
     */
    public byte[] getKey() {
        return keyData;
    }

    /**
     * Get the hex representation of the key data.
     * @return hex string of the key data
     */
    public String getHexData() {
        return Utils.bytesToHex(keyData);
    }

    /**
     * String representation of the object
     * @return String representation of the object
     */
    @Override
    public String toString() {
        return getHexData() + " (" + Arrays.toString(keyData) + ')';
    }

    /**
     * Encrypt the byte array data
     * @param data Input data
     * @return Encrypted data
     * @throws SymmetricKeyException
     */
    public byte[] encryptData(byte[] data) throws SymmetricKeyException {
        Encryptor encryptor = createEncryptor();
        byte[] encrypted = encryptor.encrypt(data);
        byte[] trailer = encryptor.encrypt(null);
        if (encrypted == null || trailer == null)
            throw new SymmetricKeyException("Cannot encrypt data");
        byte[] result = ArrayUtils.concat(encrypted, trailer);
        return result;
    }

    /**
     * Decrypt the encrypted byte array data. The encrypted data must be prefixed with the
     * IV header (16 bytes) used when encrypting the data.
     * @param data Encrypted data
     * @return Decrypted data
     * @throws SymmetricKeyException
     */
    public byte[] decryptData(byte[] data) throws SymmetricKeyException {
        if (data.length < IV_SIZE)
            throw new SymmetricKeyException("Invalid encrypted data, no IV prepended");
        byte[] iv = ArrayUtils.subarray(data, 0, IV_SIZE);
        Cipher cipher = getCipher(Cipher.DECRYPT_MODE, iv);
        try {
            return cipher.doFinal(data, iv.length, data.length - iv.length);
        } catch (Exception e) {
            throw new SymmetricKeyException(e);
        }
    }

    /**
     * Decrypt the input stream. The encrypted stream must be prefixed with the IV header
     * (16 bytes) use when encrypting the data of the given input stream.
     * @param input InputStream of the data to be decrypted
     * @return InputStream of the decrypted data
     * @throws SymmetricKeyException
     */
    public InputStream decryptStream(InputStream input) throws SymmetricKeyException {
        try {
            EncryptedInputStream encryptedInputStream = new EncryptedInputStream(input);
            byte[] iv = encryptedInputStream.getIv();
            Cipher cipher = getCipher(Cipher.DECRYPT_MODE, iv);
            return new CipherInputStream(encryptedInputStream, cipher);
        } catch (IOException e) {
            throw new SymmetricKeyException(e);
        }
    }

    /**
     * Generate an AES key of the specifies size in bytes.
     * @param size Size in bytes
     * @return An AES key
     * @throws SymmetricKeyException
     */

    private static byte[] generateKey(int size) throws SymmetricKeyException {
        if (size <= 0)
            throw new IllegalArgumentException("Size cannot be zero or less than zero.");

        try {
            SecureRandom secureRandom = new SecureRandom();
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(size * 8, secureRandom);
            return keyGenerator.generateKey().getEncoded();
        } catch (NoSuchAlgorithmException e) {
            throw new SymmetricKeyException(e);
        }
    }

    /**
     * Secure random bytes of size in bytes
     * @param size Size in bytes
     * @return Random bytes
     */
    private static byte[] secureRandom(int size) {
        if (size <= 0)
            throw new IllegalArgumentException("Size cannot be zero or less than zero.");

        SecureRandom secureRandom = new SecureRandom();
        byte[] bytes = new byte[size];
        secureRandom.nextBytes(bytes);
        return bytes;
    }

    /**
     * Get a cipher instance for either encrypt or decrypt mode with an IV header.
     * @param mode Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
     * @param iv IV header
     * @return A cipher object
     * @throws SymmetricKeyException
     */
    private Cipher getCipher(int mode, byte[] iv) throws SymmetricKeyException {
        Cipher cipher = null;
        try {
            cipher = getCipherInstance("AES/CBC/PKCS7Padding");
            if (cipher == null) {
                throw new SymmetricKeyException("Cannot get a cipher instance for AES/CBC/PKCS7Padding algorithm");
            }
            SecretKey secret = new SecretKeySpec(getKey(), "AES");
            cipher.init(mode, secret, new IvParameterSpec(iv));
        } catch (InvalidKeyException e) {
            throw new SymmetricKeyException("Couchbase Lite uses the AES 256-bit key to provide data encryption. " +
                    "Please make sure you have installed 'Java Cryptography Extension (JCE) " +
                    "Unlimited Strength Jurisdiction' Policy provided by Oracle.", e);
        } catch (SymmetricKeyException e) {
            throw e;
        } catch (Exception e) {
            throw new SymmetricKeyException(e);
        }
        return cipher;
    }

    /**
     * Get a cipher instance for the algorithm. It will try to use the Cipher from the default
     * security provider by the platform. If it couldn't find the cipher, it will try to
     * the cipher from the Bouncy Castle if the BouncyCastle library is available.
     * @param algorithm Algorithm
     * @return A cipher object
     */
    private Cipher getCipherInstance(String algorithm) {
        Cipher cipher = null;

        if (!useBCProvider) {
            try {
                cipher = Cipher.getInstance(algorithm);
            } catch (NoSuchAlgorithmException e) {
                Log.v(Log.TAG_SYMMETRIC_KEY, "Cannot find a cipher (no algorithm); will try with Bouncy Castle provider.");
            } catch (NoSuchPaddingException e) {
                Log.v(Log.TAG_SYMMETRIC_KEY, "Cannot find a cipher (no padding); will try with Bouncy Castle provider.");
            }
        }

        if (cipher == null) {
            // Register and use BouncyCastle provider if applicable:
            try {
                if (Security.getProvider("BC") == null) {
                    try {
                        Class bc = Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider");
                        Security.addProvider((Provider)bc.newInstance());
                    } catch (Exception e) {
                        Log.e(Log.TAG_SYMMETRIC_KEY, "Cannot instantiate Bouncy Castle provider", e);
                        return null;
                    }
                }
                cipher = Cipher.getInstance(algorithm, "BC");
                useBCProvider = true;
            } catch (Exception e) {
                Log.e(Log.TAG_SYMMETRIC_KEY, "Cannot find a cipher with Bouncy Castle provider", e);
            }
        }
        return cipher;
    }

    /**
     * Create an Encryptor object. The created encryptor object with use a Cipher object generated
     * with a secure random IV header.
     * @return An Encryptor object
     * @throws SymmetricKeyException
     */
    public Encryptor createEncryptor() throws SymmetricKeyException {
        return new Encryptor();
    }

    /**
     * An Encryptor class used for incrementally encrypting data. To finalize the encryption,
     * call the encrypt() method will null data.
     */
    public class Encryptor {
        private Cipher cipher;
        private boolean wroteIV;

        /**
         * Create an Encryptor object. The constructor will create an encrypt-mode cipher with
         * a secure generated IV header.
         * @throws SymmetricKeyException
         */
        public Encryptor() throws SymmetricKeyException {
            byte[] iv = secureRandom(IV_SIZE);
            cipher = getCipher(Cipher.ENCRYPT_MODE, iv);
            wroteIV = false;
        }

        /**
         * Incrementally encrypt the data. To finalize the encryption, specify null value data.
         * @param data Input data to be encrypted
         * @return Encrypted data
         * @throws SymmetricKeyException
         */
        public byte[] encrypt(byte[] data) throws SymmetricKeyException {
            return encrypt(data, 0, (data != null ? data.length : 0));
        }

        /**
         * Incrementally encrypt a subset of the data with given offset and length information.
         * To finalize the encryption, specify null value data.
         * @param data Input data to be encrypted
         * @param offset Start offset of the data to be encrypted
         * @param len Number of bytes from the start offset of the data to be encrypted
         * @return Encrypted data
         * @throws SymmetricKeyException
         */
        public byte[] encrypt(byte[] data, int offset, int len) throws SymmetricKeyException {
            byte[] dataOut;
            try {
                if (data != null)
                    dataOut = cipher.update(data, offset, len);
                else
                    dataOut = cipher.doFinal();
                if (!wroteIV) {
                    // Prepend the IV to the output data:
                    AlgorithmParameters params = cipher.getParameters();
                    byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();
                    dataOut = ArrayUtils.concat(iv, dataOut);
                    wroteIV = true;
                }
            } catch (Exception e) {
                throw new SymmetricKeyException(e);
            }
            return dataOut;
        }
    }

    /**
     * A wrapper InputStream to an encrypted InputStream to be decrypted.
     * The Initial Vector prefixed to the raw encrypted data will be extracted and accessible.
     */
    private class EncryptedInputStream extends FilterInputStream {
        // IV header extracted from the head of the stream
        private byte[] iv = null;

        /**
         * Creates a <code>FilterInputStream</code>
         * by assigning the  argument <code>in</code>
         * to the field <code>this.in</code> so as
         * to remember it for later use.
         *
         * @param in the underlying input stream, or <code>null</code> if
         *           this instance is to be created without an underlying stream.
         */
        protected EncryptedInputStream(InputStream in) throws IOException, SymmetricKeyException {
            super(in);
            if (in != null) {
                byte[] ivBuffer = new byte[IV_SIZE];
                if (in.read(ivBuffer, 0, IV_SIZE) == IV_SIZE)
                    iv = ivBuffer;
                else
                    throw new SymmetricKeyException("Invalid encrypted data, no IV prepended");
            }
        }

        /**
         * Returns Initial Vector information prefixed to the encrypted data.
         * @return IV inforamation
         */
        public byte[] getIv() {
            return iv;
        }
    }
}