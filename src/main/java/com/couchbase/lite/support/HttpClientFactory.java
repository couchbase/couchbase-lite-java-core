package com.couchbase.lite.support;

import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.cookie.Cookie;

import java.util.List;

public interface HttpClientFactory {
    HttpClient getHttpClient();

    void addCookies(List<Cookie> cookies);

    void deleteCookie(String name);

    CookieStore getCookieStore();
}
