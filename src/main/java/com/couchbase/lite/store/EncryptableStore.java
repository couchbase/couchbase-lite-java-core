/**
 * Created by Pasin Suriyentrakorn on 8/29/15.
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
 */

package com.couchbase.lite.store;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.support.action.Action;
import com.couchbase.lite.support.security.SymmetricKey;

public interface EncryptableStore {
    /**
     * Set encryption key for opening the storage.
     * @param key a SymmetricKey object
     */
    void setEncryptionKey(SymmetricKey key);

    /**
     * Return current encryption key
     * @return a SymmetricKey object
     */
    SymmetricKey getEncryptionKey();

    /**
     * Action for changing the encryption key of the storage.
     * @param newKey a new SymmetricKey object
     * @return an Action object
     */
    Action actionToChangeEncryptionKey(SymmetricKey newKey);

    /**
     * A utility method for deriving PBKDF2-SHA256 based key from the password.
     * @param password password
     * @param salt salt
     * @param rounds number of rounds
     * @return a derived PBKDF2-SHA256 key
     */

    /**
     * A utility method for deriving PBKDF2-SHA256 based key from the password.
     * @param password password
     * @param salt salt
     * @param rounds number of rounds
     * @return a derived PBKDF2-SHA256 key
     * @throws CouchbaseLiteException
     */
    byte[] derivePBKDF2SHA256Key(String password, byte[] salt, int rounds) throws CouchbaseLiteException;
}
