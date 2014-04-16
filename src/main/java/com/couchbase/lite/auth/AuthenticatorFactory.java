package com.couchbase.lite.auth;

import java.util.HashMap;
import java.util.Map;

/**
 * A factory to create instances of supported Authenticators.
 */
public class AuthenticatorFactory {

    /*
     * Creates an Authenticator that knows how to do Basic authentication.
     *
     * @param username      username
     * @param password      password
     */
    public static Authenticator createBasicAuthenticator(String username, String password) {
        return new BasicAuthenticator(username, password);
    }

    /*
     * Creates an Authenticator that knows how to do Facebook authentication.
     *
     * @param token      Facebook access token
     */
    public static Authenticator createFacebookAuthenticator(String token) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("access_token", token);
        return new TokenAuthenticator("_facebook", params);
    }

    /*
     * Creates an Authenticator that knows how to do Persona authentication.
     *
     * @param assertion     Persona Assertion
     * @param email         User email address
     */
    public static Authenticator createPersonaAuthenticator(String assertion, String email) {
        // TODO: REVIEW : Do we need email?
        Map<String, String> params = new HashMap<String, String>();
        params.put("access_token", assertion);
        return new TokenAuthenticator("_persona", params);
    }
}