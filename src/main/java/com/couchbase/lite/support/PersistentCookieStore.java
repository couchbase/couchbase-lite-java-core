/*
    Copyright (c) 2011 James Smith <james@loopj.com>
    Copyright (c) 2014 Couchbase, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.couchbase.lite.support;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.util.Log;

import org.apache.http.client.CookieStore;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.cookie.BasicClientCookie2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class PersistentCookieStore implements CookieStore {

    private static final String SYNC_GATEWAY_SESSION_COOKIE_NAME = "SyncGatewaySession";

    private static final String COOKIE_LOCAL_DOC_NAME = "PersistentCookieStore";
    private boolean omitNonPersistentCookies = false;

    private final ConcurrentHashMap<String, Cookie> cookies;

    // Weak reference to a Database.  The reason it's weak is because
    // there's a circular reference, and so the Weakref should avoid
    // the GC being thwarted.
    private WeakReference<Database> dbWeakRef;

    /**
     * Construct a persistent cookie store.
     */
    public PersistentCookieStore(Database db) {

        this.dbWeakRef = new WeakReference<Database>(db);

        cookies = new ConcurrentHashMap();

        // Load any previously stored cookies into the store
        loadPreviouslyStoredCookies(db);

        // Clear out expired cookies
        clearExpired(new Date());

    }

    private Database getDb() {
        return dbWeakRef.get();
    }

    private void loadPreviouslyStoredCookies(Database db) {
        try {
            Map<String, Object> cookiesDoc = db.getExistingLocalDocument(COOKIE_LOCAL_DOC_NAME);
            if (cookiesDoc == null) {
                return;
            }
            for (String name : cookiesDoc.keySet()) {

                // ignore special couchbase lite fields like _id and _rev
                if (name.startsWith("_")) {
                    continue;
                }

                String encodedCookie = (String) cookiesDoc.get(name);
                if (encodedCookie == null) {
                    continue;
                }
                Cookie decodedCookie = decodeCookie(encodedCookie);
                if (decodedCookie == null) {
                    continue;
                }
                if (!decodedCookie.isExpired(new Date())) {
                    cookies.put(name, decodedCookie);
                }
            }
        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, "Exception loading previously stored cookies", e);
        }
    }

    @Override
    public void addCookie(Cookie cookie) {

        if (omitNonPersistentCookies && !cookie.isPersistent())
            return;
        String name = cookie.getName() + cookie.getDomain();

        // Do we already have this cookie?  If so, don't bother.
        if (cookies.containsKey(name) && cookies.get(name).equals(cookie)) {
            return;
        }

        // WORKAROUND for Sync Gateway Session ID
        // Sync Gateway return Set-Cookie with Session ID. Cookie path is also updated, that causes mis-matching.
        // https://github.com/couchbase/couchbase-lite-java-core/issues/852
        if (SYNC_GATEWAY_SESSION_COOKIE_NAME.equalsIgnoreCase(cookie.getName()) && cookies.containsKey(name)) {
            Cookie oldCookie = cookies.get(name);
            cookie = createCookie(cookie.getName(), cookie.getValue(), cookie.getDomain(), oldCookie.getPath(), cookie.getExpiryDate(), cookie.isSecure());
        }

        // Save cookie into local store, or remove if expired
        if (!cookie.isExpired(new Date())) {
            cookies.put(name, cookie);
        } else {
            cookies.remove(name);
        }

        String encodedCookie = encodeCookie(new SerializableCookie(cookie));
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

    private Cookie createCookie(String name, String value, String domain, String path, Date expirationDate, boolean secure) {
        BasicClientCookie2 cookie = new BasicClientCookie2(name, value);
        cookie.setDomain(domain);
        cookie.setPath(path);
        cookie.setExpiryDate(expirationDate);
        cookie.setSecure(secure);
        return cookie;
    }

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
            if (cookie.isExpired(date)) {
                // Clear cookies from local store
                cookies.remove(name);

                deletePersistedCookie(name);

                // We've cleared at least one
                clearedAny = true;
            }
        }

        return clearedAny;
    }

    private void deletePersistedCookie(String name) {
        Map<String, Object> cookiesDoc = getDb().getExistingLocalDocument(COOKIE_LOCAL_DOC_NAME);
        if (cookiesDoc == null) {
            return;
        }
        cookiesDoc.remove(name);
        try {
            getDb().putLocalDocument(COOKIE_LOCAL_DOC_NAME, cookiesDoc);
        } catch (CouchbaseLiteException e) {
            Log.e(Log.TAG_SYNC, "Exception saving local doc", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Cookie> getCookies() {
        return new ArrayList(cookies.values());
    }

    /**
     * Will make PersistentCookieStore instance ignore Cookies, which are non-persistent by
     * signature (`Cookie.isPersistent`)
     *
     * @param omitNonPersistentCookies true if non-persistent cookies should be omited
     */
    public void setOmitNonPersistentCookies(boolean omitNonPersistentCookies) {
        this.omitNonPersistentCookies = omitNonPersistentCookies;
    }

    /**
     * Non-standard helper method, to delete cookie
     *
     * @param cookie cookie to be removed
     */
    public void deleteCookie(Cookie cookie) {
        String name = cookie.getName();
        cookies.remove(name);
        deletePersistedCookie(name);
    }

    /**
     * Serializes Cookie object into String
     *
     * @param cookie cookie to be encoded, can be null
     * @return cookie encoded as String
     */
    /* package */ String encodeCookie(SerializableCookie cookie) {
        if (cookie == null)
            return null;
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(os);
            outputStream.writeObject(cookie);
        } catch (Exception e) {
            Log.e(Log.TAG_SYNC, String.format("encodeCookie failed.  cookie: %s", cookie), e);
            return null;
        }
        return byteArrayToHexString(os.toByteArray());
    }

    /**
     * Returns cookie decoded from cookie string
     *
     * @param cookieString string of cookie as returned from http request
     * @return decoded cookie or null if exception occured
     */
    /* package */ Cookie decodeCookie(String cookieString) {
        Cookie cookie = null;
        try {
            byte[] bytes = hexStringToByteArray(cookieString);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            cookie = ((SerializableCookie) objectInputStream.readObject()).getCookie();
        } catch (Exception exception) {
            Log.d(Log.TAG_SYNC, String.format("decodeCookie failed.  encoded cookie: %s", cookieString), exception);
        }
        return cookie;
    }

    /**
     * Using some super basic byte array &lt;-&gt; hex conversions so we don't have to rely on any
     * large Base64 libraries. Can be overridden if you like!
     *
     * @param bytes byte array to be converted
     * @return string containing hex values
     */
    protected String byteArrayToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte element : bytes) {
            int v = element & 0xff;
            if (v < 16) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(v));
        }
        return sb.toString().toUpperCase(Locale.US);
    }

    /**
     * Converts hex values from strings to byte arra
     *
     * @param hexString string of hex-encoded values
     * @return decoded byte array
     */
    protected byte[] hexStringToByteArray(String hexString) {
        int len = hexString.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4) + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }


}
