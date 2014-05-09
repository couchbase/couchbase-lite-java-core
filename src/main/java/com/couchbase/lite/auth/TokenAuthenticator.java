package com.couchbase.lite.auth;

import java.net.URL;
import java.util.Map;

/**
 * Authenticator impl that knows how to do token based auth
 *
 * @exclude
 */
public class TokenAuthenticator extends AuthenticatorImpl {
    private String loginPath;
    private Map<String, String>loginParams;

    public TokenAuthenticator(String loginPath, Map<String, String> loginParams) {
        this.loginPath = loginPath;
        this.loginParams = loginParams;
    }

    @Override
    public boolean usesCookieBasedLogin() {
        return true;
    }

    @Override
    public Map<String, String> loginParametersForSite(URL site) {
        return loginParams;
    }

    @Override
    public String loginPathForSite(URL site) {
        String path = loginPath;
        if (path != null && !path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }
}
