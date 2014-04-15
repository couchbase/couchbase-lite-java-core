package com.couchbase.lite.auth;

import java.net.URL;
import java.util.Map;

/**
 * Created by Pasin Suriyentrakorn <pasin@couchbase.com> on 4/14/14.
 */
public abstract class AuthenticatorImpl implements Authenticator {
    public String authUserInfo() {
        return null;
    }

    public abstract boolean usesCookieBasedLogin();

    public abstract String loginPathForSite(URL site);

    public abstract Map<String, String> loginParametersForSite(URL site);
}
