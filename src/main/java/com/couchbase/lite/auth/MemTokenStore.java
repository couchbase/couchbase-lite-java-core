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

import java.net.URL;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Created by hideki on 6/22/16.
 */
public class MemTokenStore implements TokenStore {
    private Map<String, Map<String, String>> store = new HashMap<String, Map<String, String>>();

    @Override
    public Map<String, String> loadTokens(URL remoteURL) throws Exception {
        if(remoteURL == null)
            return null;
        String key = getKey(remoteURL);
        if (!store.containsKey(key))
            return null;
        return store.get(key);
    }

    @Override
    public boolean saveTokens(URL remoteURL, Map<String, String> tokens) {
        if(tokens == null)
            return deleteTokens(remoteURL);

        if(remoteURL == null)
            return false;

        String key = getKey(remoteURL);
        store.put(key, tokens);
        return true;
    }

    @Override
    public boolean deleteTokens(URL remoteURL) {
        if(remoteURL == null)
            return false;
        String key = getKey(remoteURL);
        if (!store.containsKey(key))
            return false;
        store.remove(key);
        return true;
    }

    /*package*/  String getKey(URL remoteURL) {
        if(remoteURL == null)
            throw new IllegalArgumentException("remoteURL is null");
        String account = remoteURL.toExternalForm();
        String label = String.format(Locale.ENGLISH, "%s OpenID Connect tokens", remoteURL.getHost());
        return String.format(Locale.ENGLISH, "%s%s", label, account);
    }
}
