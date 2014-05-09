package com.couchbase.lite.auth;

import java.net.URL;
import java.util.Map;

/**
 * Abstract implementation of Authenticator that all Authenticator Impls
 * should extend from
 *
 * @exclude
 */
public abstract class AuthenticatorImpl implements Authenticator {

    public String authUserInfo() {
        return null;
    }

    public abstract boolean usesCookieBasedLogin();

    public abstract String loginPathForSite(URL site);

    public abstract Map<String, String> loginParametersForSite(URL site);
}
