package com.couchbase.lite.auth;

import java.net.URL;
import java.util.Map;

public class BasicAuthenticator extends AuthenticatorImpl {
    private String username;
    private String password;

    public BasicAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public boolean usesCookieBasedLogin() {
        return true;
    }

    @Override
    public String authUserInfo() {
        if (this.username != null && this.password != null) {
            return this.username + ":" + this.password;
        }
        return super.authUserInfo();
    }

    @Override
    public String loginPathForSite(URL site) {
        return "/_session";
    }

    @Override
    public Map<String, String> loginParametersForSite(URL site) {
        return null;
    }
}
