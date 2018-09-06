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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.ConnectionPool;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

public class CouchbaseLiteHttpClientFactory implements HttpClientFactory {
    private OkHttpClient client;
    private ClearableCookieJar cookieJar;
    private SSLSocketFactory sslSocketFactory;
    private HostnameVerifier hostnameVerifier;
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
            throw new RuntimeException("SSLSocketFactory is already set");
        }
        this.sslSocketFactory = sslSocketFactory;
    }

    @InterfaceAudience.Private
    public void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
        if (this.hostnameVerifier != null) {
            throw new RuntimeException("HostnameVerifier is already set");
        }
        this.hostnameVerifier = hostnameVerifier;
    }

    ////////////////////////////////////////////////////////////
    // Implementations of HttpClientFactory
    ////////////////////////////////////////////////////////////

    @Override
    @InterfaceAudience.Private
    public void evictAllConnectionsInPool() {
        if (client != null) {
            ConnectionPool pool = client.connectionPool();
            if (pool != null)
                pool.evictAll();
        }
    }

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
            else if (isAndriod()) {
                try {
                    X509TrustManager tm = defaultTrustManager();
                    SSLSocketFactory factory = new TLSSocketFactory(null, new TrustManager[] { tm }, null);
                    builder.sslSocketFactory(factory, tm);
                } catch (GeneralSecurityException e) {
                    throw new RuntimeException(e);
                }
            }

            if (hostnameVerifier != null)
                builder.hostnameVerifier(hostnameVerifier);

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

    private static X509TrustManager defaultTrustManager() throws GeneralSecurityException {
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init((KeyStore) null);
        TrustManager[] trustManagers = factory.getTrustManagers();
        if (trustManagers.length == 0)
            throw new IllegalStateException("Cannot find the default trust manager");
        return (X509TrustManager) trustManagers[0];
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
                // https://github.com/square/okhttp/issues/2329#issuecomment-188325043
                return new X509Certificate[0];
            }
        };
        return new TLSSocketFactory(null,
                new TrustManager[]{trustManager}, null);
    }

    private static HostnameVerifier ignoreHostnameVerifier() {
        return new HostnameVerifier() {
            @Override
            public boolean verify(String s, SSLSession sslSession) {
                return true;
            }
        };
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
        // SSLSocketFactory that bypasses certificate verification.
        try {
            setSSLSocketFactory(selfSignedSSLSocketFactory());
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }

        // HostnameVerifier that bypasses hotname verification
        setHostnameVerifier(ignoreHostnameVerifier());
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

    private static boolean isAndriod() {
        return (System.getProperty("java.vm.name").equalsIgnoreCase("Dalvik"));
    }

    /**
     * Workaround to enable both TLS1.1 and TLS1.2 for Android API 16 - 19.
     * When starting to support from API 20, we could remove the workaround.
     */
    private static class TLSSocketFactory extends SSLSocketFactory {
        private SSLSocketFactory delegate;

        public TLSSocketFactory(KeyManager[] keyManagers, TrustManager[] trustManagers, SecureRandom secureRandom) throws GeneralSecurityException {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers, trustManagers, secureRandom);
            delegate = context.getSocketFactory();
        }

        public TLSSocketFactory() throws GeneralSecurityException {
            this(null, null, null);
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public Socket createSocket(Socket socket, String host, int port, boolean autoClose) throws IOException {
            return setEnabledProtocols(delegate.createSocket(socket, host, port, autoClose));
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
            return setEnabledProtocols(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException, UnknownHostException {
            return setEnabledProtocols(delegate.createSocket(host, port, localHost, localPort));
        }

        @Override
        public Socket createSocket(InetAddress address, int port) throws IOException {
            return setEnabledProtocols(delegate.createSocket(address, port));
        }

        @Override
        public Socket createSocket(InetAddress inetAddress, int port, InetAddress localAddress, int localPort) throws IOException {
            return setEnabledProtocols(delegate.createSocket(inetAddress, port, localAddress, localPort));
        }

        private Socket setEnabledProtocols(Socket socket) {
            if(socket != null && (socket instanceof SSLSocket))
                ((SSLSocket)socket).setEnabledProtocols(new String[] {"TLSv1", "TLSv1.1", "TLSv1.2"});
            return socket;
        }
    }
}
