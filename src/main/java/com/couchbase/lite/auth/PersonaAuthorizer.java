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
package com.couchbase.lite.auth;

import com.couchbase.lite.Manager;
import com.couchbase.lite.util.Base64;
import com.couchbase.lite.util.Log;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import okhttp3.Headers;

/**
 * Authorizer for the Persona decentralized-identity system. See http://persona.org
 *
 * @exclude
 */
public class PersonaAuthorizer extends BaseAuthorizer implements SessionCookieAuthorizer {

    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    public static final String TAG = Log.TAG_SYNC;

    public static final String ASSERTION_FIELD_EMAIL = "email";
    public static final String ASSERTION_FIELD_ORIGIN = "origin";
    public static final String ASSERTION_FIELD_EXPIRATION = "exp";

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    private static Map<List<String>, String> sAssertions
            = Collections.synchronizedMap(new HashMap<List<String>, String>());

    private String email;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////
    public PersonaAuthorizer(String email) {
        if (email == null)
            throw new IllegalArgumentException("email is null");
        this.email = email;
    }

    ////////////////////////////////////////////////////////////
    // Implementation of SessionCookieAuthorizer (LoginAuthorizer)
    ////////////////////////////////////////////////////////////

    @Override
    public List<Object> loginRequest() {
        String assertion = assertion();
        if (assertion == null)
            return null;
        return Arrays.asList("POST", "_persona", (Object) assertion);
    }

    @Override
    public void loginResponse(Object jsonResponse,
                              Headers headers,
                              Throwable error,
                              ContinuationBlock block) {
        // @optional
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean implementedLoginResponse() {
        return false;
    }

    ////////////////////////////////////////////////////////////
    // Public methods
    ////////////////////////////////////////////////////////////

    public String getEmailAddress() {
        return email;
    }

    public void setEmailAddress(String email) {
        this.email = email;
    }

    ////////////////////////////////////////////////////////////
    // Public static methods
    ////////////////////////////////////////////////////////////
    public static String registerAssertion(String assertion) {
        Map<String, Object> result = parseAssertion(assertion);
        if (result == null)
            return null;
        String email = (String) result.get(ASSERTION_FIELD_EMAIL);
        String origin = (String) result.get(ASSERTION_FIELD_ORIGIN);
        if (origin == null)
            throw new IllegalArgumentException("Invalid assertion, origin was null");

        // Normalize the origin URL string:
        try {
            URL originURL = new URL(origin);
            origin = originURL.toExternalForm().toLowerCase();
        } catch (MalformedURLException e) {
            String msg = String.format(Locale.ENGLISH, "Error registering assertion: %s", assertion);
            Log.e(Log.TAG_SYNC, msg, e);
            throw new IllegalArgumentException(msg, e);
        }

        List<String> key = new ArrayList<String>();
        key.add(email);
        key.add(origin);
        Log.v(Log.TAG_SYNC, "PersonaAuthorizer registering key: %s", key);
        sAssertions.put(key, assertion);
        return email;
    }


    public static String assertionForEmailAndSite(String email, URL site) {
        List<String> key = new ArrayList<String>();
        key.add(email);
        key.add(site == null ? null : site.toExternalForm().toLowerCase());
        Log.v(Log.TAG_SYNC, "PersonaAuthorizer looking up key: %s from list of sAssertions", key);
        return sAssertions.get(key);
    }

    ////////////////////////////////////////////////////////////
    // Protected & Private methods
    ////////////////////////////////////////////////////////////

    /*package*/ String assertion() {
        String assertion = assertionForEmailAndSite(email, getRemoteURL());
        if (assertion == null) {
            Log.w(TAG, "PersonaAuthorizer<%s> no assertion found for: %s", email, getRemoteURL());
            return null;
        }
        Map<String, Object> result = parseAssertion(assertion);
        if (result == null || isAssertionExpired(result)) {
            Log.w(TAG, "PersonaAuthorizer<%s> assertion invalid or expired: %s", email, assertion);
            return null;
        }
        return assertion;
    }


    ////////////////////////////////////////////////////////////
    // Protected & Private static methods
    ////////////////////////////////////////////////////////////
    private static Map decodeComponent(String component) {
        byte[] bodyData = Base64.decode(component, Base64.DEFAULT);
        if (bodyData == null)
            return null;
        try {
            return Manager.getObjectMapper().readValue(bodyData, Map.class);
        } catch (IOException e) {
            Log.w(TAG, "Failed decode component: %s", e, component);
            return null;
        }
    }

    /*package*/
    static Map<String, Object> parseAssertion(String assertion) {
        // https://github.com/mozilla/id-specs/blob/prod/browserid/index.md
        // http://self-issued.info/docs/draft-jones-json-web-token-04.html

        Map<String, Object> result = new HashMap<String, Object>();

        String[] components = assertion.split("\\.");  // split on "."
        if (components.length < 4)
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Invalid assertion given, only %d found. Expected 4+", components.length));

        Map<?, ?> body = decodeComponent(components[1]);
        Map<?, ?> principal = (Map<?, ?>) body.get("principal");
        result.put(ASSERTION_FIELD_EMAIL, principal.get("email"));

        body = decodeComponent(components[3]);
        result.put(ASSERTION_FIELD_ORIGIN, body.get("aud"));

        Long expObject = (Long) body.get("exp");
        Date expDate = new Date(expObject.longValue());
        result.put(ASSERTION_FIELD_EXPIRATION, expDate);

        return result;
    }

    private boolean isAssertionExpired(Map<String, Object> parsedAssertion) {
        Date exp;
        exp = (Date) parsedAssertion.get(ASSERTION_FIELD_EXPIRATION);
        Date now = new Date();
        if (exp.before(now)) {
            Log.w(TAG, "PersonaAuthorizer assertion for %s expired: %s", email, exp);
            return true;
        }
        return false;
    }
}
