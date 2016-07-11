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
package com.couchbase.lite.support;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.util.Log;

import java.lang.ref.WeakReference;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import okhttp3.Cookie;
import okhttp3.HttpUrl;

public class PersistentCookieJar implements ClearableCookieJar {
    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    private static final String SYNC_GATEWAY_SESSION_COOKIE_NAME = "SyncGatewaySession";
    private static final String COOKIE_LOCAL_DOC_NAME = "PersistentCookieStore";

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    private final ConcurrentHashMap<String, Cookie> cookies;

    // Weak reference to a Database.  The reason it's weak is because
    // there's a circular reference, and so the Weakref should avoid
    // the GC being thwarted.
    private final WeakReference<Database> dbWeakRef;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    public PersistentCookieJar(Database db) {
        cookies = new ConcurrentHashMap<String, Cookie>();
        this.dbWeakRef = new WeakReference<Database>(db);
        // Load any previously stored cookies into the store
        loadPreviouslyStoredCookies(db);
        // Clear out expired cookies
        clearExpired(new Date());
    }


    ////////////////////////////////////////////////////////////
    // Implementations of ClearableCookieJar
    ////////////////////////////////////////////////////////////

    @Override
    public void clear() {
        try {
            getDb().putLocalDocument(COOKIE_LOCAL_DOC_NAME, null);
        } catch (CouchbaseLiteException e) {
            Log.i(Log.TAG_SYNC, "Unable to clear Cookies: Status=" + e.getCause(), e);
        }
        // Clear cookies from local store
        cookies.clear();
    }

    @Override
    public boolean clearExpired(Date date) {
        boolean clearedAny = false;
        for (ConcurrentHashMap.Entry<String, Cookie> entry : cookies.entrySet()) {
            String name = entry.getKey();
            Cookie cookie = entry.getValue();
            if (cookie.expiresAt() < date.getTime()) {
                // Clear cookies from local store
                cookies.remove(name);
                deletePersistedCookie(name);
                // We've cleared at least one
                clearedAny = true;
            }
        }
        return clearedAny;
    }

    @Override
    public void saveFromResponse(HttpUrl httpUrl, List<okhttp3.Cookie> list) {
        for (okhttp3.Cookie cookie : list) {
            String name = cookie.name() + cookie.domain();

            // Do we already have this cookie?  If so, don't bother.
            if (cookies.containsKey(name) && cookies.get(name).equals(cookie)) {
                return;
            }

            // WORKAROUND for Sync Gateway Session ID
            // Sync Gateway return Set-Cookie with Session ID. Cookie path is also updated, that causes mis-matching.
            // https://github.com/couchbase/couchbase-lite-java-core/issues/852
            if (SYNC_GATEWAY_SESSION_COOKIE_NAME.equalsIgnoreCase(cookie.name()) && cookies.containsKey(name)) {
                Cookie oldCookie = cookies.get(name);
                cookie = createCookie(cookie.name(), cookie.value(), cookie.domain(), oldCookie.path(), cookie.expiresAt());
            }

            // Save cookie into local store, or remove if expired
            if (cookie.expiresAt() > System.currentTimeMillis())
                cookies.put(name, cookie);
            else
                cookies.remove(name);

            String encodedCookie = new SerializableCookie().encode(cookie);
            Map<String, Object> cookiesDoc = getDb().getExistingLocalDocument(COOKIE_LOCAL_DOC_NAME);
            if (cookiesDoc == null) {
                cookiesDoc = new HashMap<String, Object>();
            }

            Log.v(Log.TAG_SYNC, "Saving cookie: %s w/ encoded value: %s", name, encodedCookie);

            cookiesDoc.put(name, encodedCookie);

            try {
                getDb().putLocalDocument(COOKIE_LOCAL_DOC_NAME, cookiesDoc);
            } catch (CouchbaseLiteException e) {
                Log.e(Log.TAG_SYNC, "Exception saving local doc", e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @return return all cookies if httpUrl is null, otherwise return matched cookies.
     */
    @Override
    public List<okhttp3.Cookie> loadForRequest(HttpUrl httpUrl) {
        List<Cookie> list = new ArrayList<Cookie>();
        if (httpUrl == null) {
            list.addAll(cookies.values());
        } else {
            for (Cookie cookie : cookies.values()) {
                if (cookie.matches(httpUrl))
                    list.add(cookie);
            }
        }
        return list;
    }

    ////////////////////////////////////////////////////////////
    // Public Methods
    ////////////////////////////////////////////////////////////

    /**
     * Non-standard helper method, to delete cookie
     *
     * @param cookie cookie to be removed
     */
    public void deleteCookie(Cookie cookie) {
        cookies.remove(cookie.name());
        deletePersistedCookie(cookie.name());
    }

    ////////////////////////////////////////////////////////////
    // Protected or Private Methods
    ////////////////////////////////////////////////////////////

    private Database getDb() {
        return dbWeakRef.get();
    }

    private void loadPreviouslyStoredCookies(Database db) {
        Map<String, Object> cookiesDoc = db.getExistingLocalDocument(COOKIE_LOCAL_DOC_NAME);
        if (cookiesDoc == null)
            return;
        for (String name : cookiesDoc.keySet()) {
            // ignore special couchbase lite fields like _id and _rev
            if (name.startsWith("_"))
                continue;
            String encodedCookie = (String) cookiesDoc.get(name);
            if (encodedCookie == null)
                continue;
            Cookie decodedCookie = new SerializableCookie().decode(encodedCookie);
            if (decodedCookie == null)
                continue;
            // NOTE: no check for expiration
            cookies.put(name, decodedCookie);
        }
    }

    private static Cookie createCookie(String name, String value, String domain, String path, Date expirationDate) {
        return createCookie(name, value, domain, path, expirationDate.getTime());
    }

    private static Cookie createCookie(String name, String value, String domain, String path, long expiresAt) {
        return new Cookie.Builder()
                .domain(domain)
                .path(path)
                .expiresAt(expiresAt)
                .name(name)
                .value(value)
                .build();
    }

    private void deletePersistedCookie(String name) {
        Map<String, Object> cookiesDoc = getDb().getExistingLocalDocument(COOKIE_LOCAL_DOC_NAME);
        if (cookiesDoc == null)
            return;
        cookiesDoc.remove(name);
        try {
            getDb().putLocalDocument(COOKIE_LOCAL_DOC_NAME, cookiesDoc);
        } catch (CouchbaseLiteException e) {
            Log.e(Log.TAG_SYNC, "Exception saving local doc", e);
            throw new RuntimeException(e);
        }
    }
}
