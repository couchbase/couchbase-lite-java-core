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

import com.couchbase.lite.internal.InterfaceAudience;

import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

public class CouchbaseLiteHttpClientFactory implements HttpClientFactory {
    private OkHttpClient client;
    private ClearableCookieJar cookieJar;
    private SSLSocketFactory sslSocketFactory;

    private boolean followRedirects = true;

    // deprecated
    public static int DEFAULT_SO_TIMEOUT_SECONDS = 40; // 40 sec (previously it was 5 min)
    // heartbeat value 30sec + 10 sec

    // OkHttp Default Timeout is 10 sec for all timeout settings
    public static int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10;
    public static int DEFAULT_READ_TIMEOUT = DEFAULT_SO_TIMEOUT_SECONDS;
    public static int DEFAULT_WRITE_TIMEOUT = 10;

    /**
     * Constructor
     */
    public CouchbaseLiteHttpClientFactory(ClearableCookieJar cookieJar) {
        this.cookieJar = cookieJar;
    }

    /**
     * @param sslSocketFactory This is to open up the system for end user to inject
     *                         the sslSocket factories with their custom KeyStore
     */
    @InterfaceAudience.Private
    public void setSSLSocketFactory(SSLSocketFactory sslSocketFactory) {
        if (this.sslSocketFactory != null) {
            throw new RuntimeException("SSLSocketFactory already set");
        }
        this.sslSocketFactory = sslSocketFactory;
    }

    ////////////////////////////////////////////////////////////
    // Implementations of HttpClientFactory
    ////////////////////////////////////////////////////////////

    @Override
    @InterfaceAudience.Private
    synchronized public OkHttpClient getOkHttpClient() {
        if (client == null) {
            OkHttpClient.Builder builder = new OkHttpClient.Builder();

            // timeout settings
            builder.connectTimeout(DEFAULT_CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .writeTimeout(DEFAULT_WRITE_TIMEOUT, TimeUnit.SECONDS)
                    .readTimeout(DEFAULT_READ_TIMEOUT, TimeUnit.SECONDS);

            if (sslSocketFactory != null)
                builder.sslSocketFactory(sslSocketFactory);

            // synchronize access to the cookieStore in case there is another
            // thread in the middle of updating it.  wait until they are done so we get their changes.
            builder.cookieJar(cookieJar);

            if (!isFollowRedirects())
                builder.followRedirects(false);

            client = builder.build();
        }
        return client;
    }

    @Override
    @InterfaceAudience.Private
    synchronized public void addCookies(List<Cookie> cookies) {
        if (cookieJar != null) {
            // TODO: HttpUrl parameter should be revisited.
            cookieJar.saveFromResponse(null, cookies);
        }
    }

    @Override
    @InterfaceAudience.Private
    synchronized public void deleteCookie(String name) {
        // since CookieStore does not have a way to delete an individual cookie, do workaround:
        // 1. get all cookies
        // 2. filter list to strip out the one we want to delete
        // 3. clear cookie store
        // 4. re-add all cookies except the one we want to delete
        if (cookieJar == null)
            return;

        List<Cookie> cookies = cookieJar.loadForRequest(null);
        List<Cookie> retainedCookies = new ArrayList<Cookie>();
        for (Cookie cookie : cookies) {
            if (!cookie.name().equals(name))
                retainedCookies.add(cookie);
        }
        cookieJar.clear();

        // TODO: HttpUrl parameter should be revisited.
        cookieJar.saveFromResponse(null, retainedCookies);
    }

    static private boolean isMatch(Cookie cookie, URL url) {
        return cookie.matches(HttpUrl.get(url));
    }

    @Override
    @InterfaceAudience.Private
    synchronized public void deleteCookie(URL url) {
        // since CookieStore does not have a way to delete an individual cookie, do workaround:
        // 1. get all cookies
        // 2. filter list to strip out the one we want to delete
        // 3. clear cookie store
        // 4. re-add all cookies except the one we want to delete
        if (cookieJar == null)
            return;

        List<Cookie> cookies = cookieJar.loadForRequest(null);
        List<Cookie> retainedCookies = new ArrayList<Cookie>();
        for (Cookie cookie : cookies) {
            // matching rely on OkHttp's matching logic
            // https://square.github.io/okhttp/3.x/okhttp/okhttp3/Cookie.html#matches-okhttp3.HttpUrl-
            if (!cookie.matches(HttpUrl.get(url)))
                retainedCookies.add(cookie);
        }
        cookieJar.clear();

        // TODO: HttpUrl parameter should be revisited.
        cookieJar.saveFromResponse(null, retainedCookies);
    }

    @Override
    @InterfaceAudience.Private
    synchronized public void resetCookieStore() {
        if (cookieJar == null)
            return;
        cookieJar.clear();
    }

    @Override
    @InterfaceAudience.Private
    public CookieJar getCookieStore() {
        return cookieJar;
    }

    private static SSLSocketFactory selfSignedSSLSocketFactory() throws GeneralSecurityException {
        TrustManager trustManager = new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType)
                    throws CertificateException {
            }

            public void checkServerTrusted(X509Certificate[] chain, String authType)
                    throws CertificateException {
            }

            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[]{trustManager}, null);
        return sslContext.getSocketFactory();
    }

    /**
     * This is a convenience method to allow couchbase lite to connect to servers
     * that use self-signed SSL certs.
     * <p/>
     * *DO NOT USE THIS IN PRODUCTION*
     * <p/>
     * For more information, see:
     * <p/>
     * https://github.com/couchbase/couchbase-lite-java-core/pull/9
     */
    @InterfaceAudience.Public
    public void allowSelfSignedSSLCertificates() {
        try {
            setSSLSocketFactory(selfSignedSSLSocketFactory());
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is for unit tests only.
     */
    public boolean isFollowRedirects() {
        return followRedirects;
    }

    /**
     * This method is for unit tests only.
     */
    public void setFollowRedirects(boolean followRedirects) {
        this.followRedirects = followRedirects;
    }
}
