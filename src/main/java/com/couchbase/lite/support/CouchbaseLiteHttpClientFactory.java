package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.internal.InterfaceAudience;

import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;

import java.util.ArrayList;
import java.util.List;

public class CouchbaseLiteHttpClientFactory implements HttpClientFactory {

    private CookieStore cookieStore;

    private SSLSocketFactory sslSocketFactory;

    private BasicHttpParams basicHttpParams;

    public static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_SO_TIMEOUT_SECONDS = 60 * 5;


    /**
     * Constructor
     */
    public CouchbaseLiteHttpClientFactory(CookieStore cookieStore) {
        this.cookieStore = cookieStore;
    }

    /**
     * @param sslSocketFactoryFromUser This is to open up the system for end user to inject the sslSocket factories with their
     *                                 custom KeyStore
     */
    @InterfaceAudience.Private
    public void setSSLSocketFactory(SSLSocketFactory sslSocketFactoryFromUser) {
        if (sslSocketFactory != null) {
            throw new RuntimeException("SSLSocketFactory already set");
        }
        sslSocketFactory = sslSocketFactoryFromUser;
    }

    @InterfaceAudience.Private
    public void setBasicHttpParams(BasicHttpParams basicHttpParams) {
        this.basicHttpParams = basicHttpParams;
    }

    @Override
    @InterfaceAudience.Private
    public HttpClient getHttpClient() {

        // workaround attempt for issue #81
        // it does not seem like _not_ using the ThreadSafeClientConnManager actually
        // caused any problems, but it seems wise to use it "just in case", since it provides
        // extra safety and there are no observed side effects.

        if (basicHttpParams == null) {
            basicHttpParams = new BasicHttpParams();
            HttpConnectionParams.setConnectionTimeout(basicHttpParams, DEFAULT_CONNECTION_TIMEOUT_SECONDS * 1000);
            HttpConnectionParams.setSoTimeout(basicHttpParams, DEFAULT_SO_TIMEOUT_SECONDS * 1000);
        }

        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
        final SSLSocketFactory sslSocketFactory = SSLSocketFactory.getSocketFactory();
        schemeRegistry.register(new Scheme("https", this.sslSocketFactory == null ? sslSocketFactory : this.sslSocketFactory, 443));
        ClientConnectionManager cm = new ThreadSafeClientConnManager(basicHttpParams, schemeRegistry);

        DefaultHttpClient client = new DefaultHttpClient(cm, basicHttpParams);

        // synchronize access to the cookieStore in case there is another
        // thread in the middle of updating it.  wait until they are done so we get their changes.
        synchronized (this) {
            client.setCookieStore(cookieStore);
        }
        return client;

    }

    @InterfaceAudience.Private
    public void addCookies(List<Cookie> cookies) {
        if (cookieStore == null) {
            return;
        }
        synchronized (this) {
            for (Cookie cookie : cookies) {
                cookieStore.addCookie(cookie);
            }
        }
    }

    public void deleteCookie(String name) {
        // since CookieStore does not have a way to delete an individual cookie, do workaround:
        // 1. get all cookies
        // 2. filter list to strip out the one we want to delete
        // 3. clear cookie store
        // 4. re-add all cookies except the one we want to delete
        if (cookieStore == null) {
            return;
        }
        List<Cookie> cookies = cookieStore.getCookies();
        List<Cookie> retainedCookies = new ArrayList<Cookie>();
        for (Cookie cookie : cookies) {
            if (!cookie.getName().equals(name)) {
                retainedCookies.add(cookie);
            }
        }
        cookieStore.clear();
        for (Cookie retainedCookie : retainedCookies) {
            cookieStore.addCookie(retainedCookie);
        }
    }

    @InterfaceAudience.Private
    public CookieStore getCookieStore() {
        return cookieStore;
    }

}
