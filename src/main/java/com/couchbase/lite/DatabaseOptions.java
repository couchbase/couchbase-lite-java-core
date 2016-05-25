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
package com.couchbase.lite;

/**
 * Options for opening a database. All properties default to false or null.
 */
public class DatabaseOptions {
    private boolean create = false;

    private boolean readOnly = false;

    private String storageType = null;

    private Object encryptionKey = null;

    /**
     * Set Create option. If set to True, opening a database will create the database
     * if it doesn't exist.
     */
    public void setCreate(boolean create) {
        this.create = create;
    }

    /**
     * Get Create option value.
     * @return Create option value.
     */
    public boolean isCreate() {
        return create;
    }

    /**
     * Set ReadOnly option. If set to True, the database will be opened in read-only mode.
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Get ReadOnly option value
     * @return ReadOnly option value
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * <p>
     *     Set the underlying storage engine to use. Legal values are @"SQLite", @"ForestDB", or nil.
     * </p>
     * <ul>
     *     <li>
     *         If the database is being created, the given storage engine will be used,
     *         or the default if the value is null
     *     </li>
     *     <li>
     *         If the database exists, and the value is not null, the database will be upgraded to that
     *         storage engine if possible. (SQLite-to-ForestDB upgrades are supported.)
     *     </li>
     * </ul>
     */
    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    /**
     * Get the storage type.
     * @return Storage type.
     */
    public String getStorageType() {
        return storageType;
    }

    /**
     * <p>
     *     Set a key to encrypt the database with. If the database does not exist and is being created,
     *     it will use this key, and the same key must be given every time it's opened.
     * </p>
     * <ul>
     *     <li>
     *         The primary form of key is an NSData object 32 bytes in length: this is interpreted as a raw
     *         AES-256 key. To create a key, generate random data using a secure cryptographic randomizer
     *         like SecRandomCopyBytes or CCRandomGenerateBytes.
     *     </li>
     *     <li>
     *         Alternatively, the value may be an NSString containing a passphrase. This will be run through
     *         64,000 rounds of the PBKDF algorithm to securely convert it into an AES-256 key.
     *     </li>
     *     <li>
     *         A default null value, of course, means the database is unencrypted.
     *     </li>
     * </ul>
     */
    public void setEncryptionKey(Object encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    /**
     * Get the encryption key.
     * @return Encryption key.
     */
    public Object getEncryptionKey() {
        return encryptionKey;
    }
}
