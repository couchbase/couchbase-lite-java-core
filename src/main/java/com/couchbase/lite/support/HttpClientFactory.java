package com.couchbase.lite.support;

import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.cookie.Cookie;

import java.util.List;

public interface HttpClientFactory {
	HttpClient getHttpClient();
    public void addCookies(List<Cookie> cookies);
    public void deleteCookie(String name);
    public CookieStore getCookieStore();
}
