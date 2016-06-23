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
package com.couchbase.lite.auth;

import com.couchbase.lite.util.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.util.Map;

/**
 * Example of TokenStore implementation with File.
 * This implementation just serializes Map object to file
 */
public class FileTokenStore implements ITokenStore {
    public static final String TAG = Log.TAG_SYNC;

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////
    private File tokenStore;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    public FileTokenStore(File tokenStore) {
        this.tokenStore = tokenStore;
    }

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    @Override
    public Map<String, String> loadTokens(URL remoteURL) throws Exception {
        FileInputStream fins = new FileInputStream(tokenStore);
        try {
            ObjectInputStream ois = new ObjectInputStream(fins);
            try {
                return (Map<String, String>) ois.readObject();
            } finally {
                ois.close();
            }
        } finally {
            fins.close();
        }
    }

    @Override
    public boolean saveTokens(URL remoteURL, Map<String, String> tokens) {
        if (tokens == null)
            return deleteTokens(remoteURL);

        try {
            FileOutputStream fos = new FileOutputStream(tokenStore);
            try {
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                try {
                    oos.writeObject(tokens);
                } finally {
                    oos.close();
                }
            } finally {
                fos.close();
            }
        } catch (IOException ioe) {
            Log.e(TAG, "Error in saveTokens()", ioe);
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteTokens(URL remoteURL) {
        if (tokenStore.exists())
            return tokenStore.delete();
        return false;
    }
}
