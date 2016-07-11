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
    public Map<String, String> loadTokens(URL remoteURL, String localUUID) throws Exception {
        if (remoteURL == null)
            return null;
        String key = getKey(remoteURL, localUUID);
        if (!store.containsKey(key))
            return null;
        return store.get(key);
    }

    @Override
    public boolean saveTokens(URL remoteURL, String localUUID, Map<String, String> tokens) {
        if (tokens == null)
            return deleteTokens(remoteURL, localUUID);

        if (remoteURL == null)
            return false;

        String key = getKey(remoteURL, localUUID);
        store.put(key, tokens);
        return true;
    }

    @Override
    public boolean deleteTokens(URL remoteURL, String localUUID) {
        if (remoteURL == null)
            return false;
        String key = getKey(remoteURL, localUUID);
        if (!store.containsKey(key))
            return false;
        store.remove(key);
        return true;
    }

    /*package*/  String getKey(URL remoteURL, String localUUID) {
        if (remoteURL == null)
            throw new IllegalArgumentException("remoteURL is null");
        String service = remoteURL.toExternalForm();
        String label = String.format(Locale.ENGLISH, "%s OpenID Connect tokens", remoteURL.getHost());
        if (localUUID == null)
            return String.format(Locale.ENGLISH, "%s%s", label, service);
        else
            return String.format(Locale.ENGLISH, "%s%s%s", label, service, localUUID);
    }
}
