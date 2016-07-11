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

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Status;
import com.couchbase.lite.replicator.RemoteRequestResponseException;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.URIUtils;
import com.couchbase.lite.util.URLUtils;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import okhttp3.Headers;
import okhttp3.Request;


public class OpenIDConnectAuthorizer extends BaseAuthorizer
        implements CustomHeadersAuthorizer, SessionCookieAuthorizer {

    private static final String TAG = Log.TAG_SYNC;

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    private String username;                     // SG user name

    // internal
    protected OIDCLoginCallback loginCallback;    // App-provided callback to log into the OP
    protected TokenStore tokenStore;

    protected URL authURL;                        // The OIDC authentication URL redirected to by the OP
    protected String IDToken;                     // Persistent ID token, when logged-in
    protected String refreshToken;                // Persistent refresh token, when logged-in or refreshing login
    protected boolean haveSessionCookie;          // YES if the server set a login session cookie

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////
    public OpenIDConnectAuthorizer(OIDCLoginCallback callback, TokenStore tokenStore) {
        this.loginCallback = callback;
        this.tokenStore = tokenStore;
    }

    ////////////////////////////////////////////////////////////
    // Public methods
    ////////////////////////////////////////////////////////////
    @Override
    public String toString() {
        return String.format(Locale.ENGLISH, "OpenIDConnectAuthorizer[%s]", getRemoteURL());
    }

    ////////////////////////////////////////////////////////////
    // Implementations of ICustomHeadersAuthorizer
    ////////////////////////////////////////////////////////////

    // Auth phase (when we have the ID token):

    @Override
    public boolean authorizeURLRequest(Request.Builder builder) {
        loadTokens();
        if (IDToken != null && !haveSessionCookie) {
            String auth = String.format(Locale.ENGLISH, "Bearer ", IDToken);
            builder.addHeader("Authorization", auth);
            return true;
        } else {
            return false;
        }
    }

    ////////////////////////////////////////////////////////////
    // Implementations of SessionCookieAuthorizer(LoginAuthorizer)
    ////////////////////////////////////////////////////////////

    // #pragma mark - LOGIN:

    @Override
    public List<Object> loginRequest() {
        loadTokens();

        // If we got here, 'GET _session' failed, so there's no valid session cookie or ID token.
        IDToken = null;
        haveSessionCookie = false;

        String path;
        if (refreshToken != null)
            path = String.format(Locale.ENGLISH, "_oidc_refresh?refresh_token=%s",
                    URIUtils.encode(refreshToken));
        else if (authURL != null)
            path = String.format(Locale.ENGLISH, "_oidc_callback?%s",
                    authURL.getQuery());
        else
            path = "_oidc_challenge?offline=true";
        // Note: casting to Object makes Arrays.asList() return List<Object>
        return Arrays.asList("GET", (Object) path);
    }

    @Override
    public void loginResponse(Object jsonResponse,
                              Headers headers,
                              Throwable error,
                              ContinuationBlock block) {
        if (error != null && (!(error instanceof RemoteRequestResponseException) ||
                ((RemoteRequestResponseException) error).getCode() != 401)) {
            block.call(false, error);
            return;
        }

        if (refreshToken != null || authURL != null) {
            // Logging in with an authURL from the OP, or refreshing the ID token:
            if (error != null) {
                authURL = null;
                if (refreshToken != null) {
                    // Refresh failed; go back to login state:
                    refreshToken = null;
                    username = null;
                    deleteTokens();
                    block.call(true, null);
                }
            } else {
                Map response = (Map) jsonResponse;
                if (refreshToken != null && response.get("refresh_token") == null) {
                    // The response from a refresh may not contain the refresh token, so to avoid
                    // losing it, add it to the response map that will be saved to the key store:
                    response = new HashMap<String, Object>(response);
                    response.put("refresh_token", refreshToken);
                }

                // Generated or refreshed ID token:
                if (parseTokens(response)) {
                    Log.v(TAG, "%s: Logged in as %s !", this.getClass().getName(), username);
                    saveTokens(response);
                } else {
                    error = new CouchbaseLiteException("Server didn't return a refreshed ID token",
                            Status.UPSTREAM_ERROR);
                }
            }
        } else {
            // Login challenge: get the info & ask the app callback to log into the OP:
            String login = null;
            RemoteRequestResponseException rrre = (RemoteRequestResponseException) error;
            Map challenge = rrre.getUserInfo() != null ?
                    (Map) rrre.getUserInfo().get("AuthChallenge") : null;
            if (challenge != null)
                if ("OIDC".equals(challenge.get("Scheme")))
                    login = (String) challenge.get("login");
            if (login != null) {
                Log.v(TAG, "OpenIDConnectAuthorizer: Got OpenID Connect login URL: <%s>", login);
                URL loginURL = null;
                try {
                    loginURL = new URL(login);
                    continueAsyncLoginWithURL(loginURL, block);
                    return; // don't call the continuation block yet
                } catch (MalformedURLException e) {
                    Log.e(TAG, "Unknown Error", e);
                    error = new CouchbaseLiteException(Status.UNKNOWN);
                }
            } else {
                error = new CouchbaseLiteException("Server didn't provide an OpenID login URL",
                        Status.UPSTREAM_ERROR);
            }
        }

        // by default, keep going immediately:
        block.call(false, error);
    }

    @Override
    public boolean implementedLoginResponse() {
        return true;
    }

    ////////////////////////////////////////////////////////////
    // Implementation of Authorizer
    ////////////////////////////////////////////////////////////

    @Override
    public boolean removeStoredCredentials() {
        if (!deleteTokens())
            return false;
        IDToken = null;
        refreshToken = null;
        haveSessionCookie = false;
        authURL = null;
        return true;
    }

    @Override
    public String getUsername() {
        // @optional
        return username;
    }

    ////////////////////////////////////////////////////////////
    // Setter/Getter
    ////////////////////////////////////////////////////////////

    public TokenStore getTokenStore() {
        return tokenStore;
    }

    public void setTokenStore(TokenStore tokenStore) {
        this.tokenStore = tokenStore;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getIDToken() {
        return IDToken;
    }

    public void setIDToken(String IDToken) {
        this.IDToken = IDToken;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    ////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////
    public static boolean forgetIDTokensForServer(URL serverURL, TokenStore tokenStore) {
        OpenIDConnectAuthorizer authorizer = new OpenIDConnectAuthorizer(null, tokenStore);
        authorizer.setRemoteURL(serverURL);
        // Deliberately don't set auth.localUUID. This will leave kSecAttrAccount unset in the
        // dictionary passed to SecItemDelete, deleting keychain items for all accounts (databases).
        return authorizer.deleteTokens();
    }

    ////////////////////////////////////////////////////////////
    // package/protected/private methods
    ////////////////////////////////////////////////////////////

    /*package*/ boolean loadTokens() {
        if (tokenStore == null)
            return false;

        try {
            return parseTokens(tokenStore.loadTokens(getRemoteURL(), getLocalUUID()));
        } catch (Exception e) {
            Log.w(TAG, "Error in loadTokens()", e);
            return false;
        }
    }

    /*package*/  boolean saveTokens(Map<String, String> tokens) {
        if (tokenStore == null)
            return false;
        return tokenStore.saveTokens(getRemoteURL(), getLocalUUID(), tokens);
    }

    /*package*/  boolean deleteTokens() {
        if (tokenStore == null)
            return false;
        return tokenStore.deleteTokens(getRemoteURL(), getLocalUUID());
    }

    private boolean parseTokens(Map<String, String> tokens) {
        if (tokens == null) {
            // If there are no tokens in the store, the tokens will be null:
            return false;
        }

        String idToken = tokens.get("id_token");
        if (idToken == null) {
            Log.v(TAG, "OpenIDConnectAuthorizer: the parsed token doesn't have the ID Token");
            return false;
        }

        IDToken = idToken;
        refreshToken = tokens.get("refresh_token");
        username = tokens.get("name");
        haveSessionCookie = tokens.containsKey("session_id");

        return true;
    }

    private void continueAsyncLoginWithURL(URL loginURL, final ContinuationBlock block) {
        Log.v(TAG, "OpenIDConnectAuthorizer: Calling app login callback block...");
        final URL remoteURL = this.getRemoteURL();
        final URL redirectBaseURL = extractRedirectURL(loginURL);
        if (loginCallback != null)
            loginCallback.callback(loginURL, redirectBaseURL, new OIDCLoginContinuation() {
                @Override
                public void callback(URL url, Throwable error) {
                    if (url != null) {
                        Log.v(TAG, "OpenIDConnectAuthorizer: App login callback returned authURL " +
                                "<%s>", url.toExternalForm());
                        // Verify that the authURL matches the site:
                        if (remoteURL == null ||
                                url.getHost().compareToIgnoreCase(remoteURL.getHost()) != 0 ||
                                url.getPort() != remoteURL.getPort()) {
                            Log.w(TAG, "OpenIDConnectAuthorizer: App-provided authURL <%s> " +
                                    "doesn't match server URL; ignoring it", url.toExternalForm());
                            url = null;
                            error = new RemoteRequestResponseException(
                                    RemoteRequestResponseException.BAD_URL, null, null);
                        }
                    }
                    if (url != null) {
                        authURL = url;
                        block.call(true, null);
                    } else {
                        if (error == null)
                            error = new RemoteRequestResponseException(
                                    RemoteRequestResponseException.USER_DENIED_AUTH, null, null);
                        Log.w(TAG, "OpenIDConnectAuthorizer: App login callback returned error=" + error);
                        block.call(false, error);
                    }
                }
            });
    }

    private static URL extractRedirectURL(URL loginURL) {
        try {
            Map<String, List<String>> queries = URLUtils.splitQuery(loginURL);
            if (queries.containsKey("redirect_uri") && queries.get("redirect_uri").size() > 0)
                try {
                    return new URL(queries.get("redirect_uri").get(0));
                } catch (MalformedURLException e) {
                    Log.w(TAG, "Invalid URL: redirect_uri=<%s>", e, queries.get("redirect_uri").get(0));
                    return null;
                }
            return null;
        } catch (UnsupportedEncodingException e) {
            Log.w(TAG, "Invalid URL: loginURL=<%s>", e, loginURL);
            return null;
        }
    }
}
