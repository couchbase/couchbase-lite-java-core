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
package com.couchbase.lite.router;

import com.couchbase.lite.AsyncTask;
import com.couchbase.lite.BlobStoreWriter;
import com.couchbase.lite.ChangesOptions;
import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.Document;
import com.couchbase.lite.DocumentChange;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Mapper;
import com.couchbase.lite.Misc;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryOptions;
import com.couchbase.lite.QueryRow;
import com.couchbase.lite.Reducer;
import com.couchbase.lite.ReplicationFilter;
import com.couchbase.lite.Revision;
import com.couchbase.lite.RevisionList;
import com.couchbase.lite.Status;
import com.couchbase.lite.TransactionalTask;
import com.couchbase.lite.View;
import com.couchbase.lite.auth.FacebookAuthorizer;
import com.couchbase.lite.auth.PersonaAuthorizer;
import com.couchbase.lite.internal.AttachmentInternal;
import com.couchbase.lite.internal.Body;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.replicator.RemoteRequestResponseException;
import com.couchbase.lite.replicator.Replication;
import com.couchbase.lite.replicator.Replication.ChangeEvent;
import com.couchbase.lite.replicator.Replication.ChangeListener;
import com.couchbase.lite.replicator.Replication.ReplicationStatus;
import com.couchbase.lite.replicator.ReplicationState;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.store.Store;
import com.couchbase.lite.support.RevisionUtils;
import com.couchbase.lite.support.Version;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.StreamUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Router implements Database.ChangeListener, Database.DatabaseListener {

    public static final String TAG = Log.TAG_ROUTER;

    private static final long MIN_HEARTBEAT = 5000; // 5 second

    private static final String CONTENT_TYPE_JSON = "application/json";

    /**
     * Options for what metadata to include in document bodies
     */
    private enum TDContentOptions {
        TDIncludeAttachments,
        TDIncludeConflicts,
        TDIncludeRevs,
        TDIncludeRevsInfo,
        TDIncludeLocalSeq,
        TDNoBody,
        TDBigAttachmentsFollow,
        TDNoAttachments
    }

    private Manager manager;
    private Database db;
    private URLConnection connection;
    private Map<String, String> queries;
    private boolean changesIncludesDocs = false;
    private boolean changesIncludesConflicts = false;
    private RouterCallbackBlock callbackBlock;
    private boolean responseSent = false;
    private ReplicationFilter changesFilter;
    Map<String, Object> changesFilterParams = null;
    private boolean longpoll = false;
    private boolean waiting = false;
    private URL source = null;
    private Timer timer = null; // timer for heartbeat
    private boolean dontOverwriteBody = false;

    private final Object databaseChangesLongpollLock = new Object();

    public static String getVersionString() {
        return Version.getVersion();
    }

    public Router(Manager manager, URLConnection connection) {
        this.manager = manager;
        this.connection = connection;
    }

    @Override
    protected void finalize() throws Throwable {
        stop();
        super.finalize();
    }

    public void setCallbackBlock(RouterCallbackBlock callbackBlock) {
        this.callbackBlock = callbackBlock;
    }

    private Map<String, String> getQueries() {
        if (queries == null) {
            String queryString = connection.getURL().getQuery();
            if (queryString != null && queryString.length() > 0) {
                queries = new HashMap<String, String>();
                for (String component : queryString.split("&")) {
                    int location = component.indexOf('=');
                    if (location > 0) {
                        String key = component.substring(0, location);
                        String value = component.substring(location + 1);
                        queries.put(key, value);
                    }
                }
            }
        }
        return queries;
    }

    private static boolean getBooleanValueFromBody(String paramName, Map<String, Object> bodyDict,
                                                   boolean defaultVal) {
        boolean value = defaultVal;
        if (bodyDict.containsKey(paramName)) {
            value = Boolean.TRUE.equals(bodyDict.get(paramName));
        }
        return value;
    }

    private String getQuery(String param) {
        Map<String, String> queries = getQueries();
        if (queries != null) {
            String value = queries.get(param);
            if (value != null) {
                return URLDecoder.decode(value);
            }
        }
        return null;
    }

    private boolean getBooleanQuery(String param) {
        String value = getQuery(param);
        return (value != null) && !"false".equals(value) && !"0".equals(value);
    }

    private int getIntQuery(String param, int defaultValue) {
        int result = defaultValue;
        String value = getQuery(param);
        if (value != null) {
            try {
                result = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                //ignore, will return default value
            }
        }

        return result;
    }

    private List parseJSONRevArrayQuery(String param) {
        Object obj = getJSONQuery(param);
        if (obj == null) return null;
        if (obj instanceof List)
            return (List) obj;
        return null;
    }

    private Object getJSONQuery(String param) {
        String value = getQuery(param);
        if (value == null) {
            return null;
        }
        Object result = null;
        try {
            result = Manager.getObjectMapper().readValue(value, Object.class);
        } catch (Exception e) {
            Log.w("Unable to parse JSON Query", e);
        }
        return result;
    }

    private boolean cacheWithEtag(String etag) {
        String eTag = String.format(Locale.ENGLISH, "\"%s\"", etag);
        connection.getResHeader().add("Etag", eTag);
        String requestIfNoneMatch = connection.getRequestProperty("If-None-Match");
        return eTag.equals(requestIfNoneMatch);
    }

    private Map<String, Object> getBodyAsDictionary() throws CouchbaseLiteException {
        // check if content-type is `application/json`
        String contentType = getRequestHeaderContentType();
        if (contentType != null && !contentType.equals(CONTENT_TYPE_JSON))
            throw new CouchbaseLiteException(Status.NOT_ACCEPTABLE);

        // parse body text
        InputStream contentStream = connection.getRequestInputStream();
        try {
            return Manager.getObjectMapper().readValue(contentStream, Map.class);
        } catch (JsonParseException jpe) {
            throw new CouchbaseLiteException(Status.BAD_JSON);
        } catch (JsonMappingException jme) {
            throw new CouchbaseLiteException(Status.BAD_JSON);
        } catch (IOException ioe) {
            throw new CouchbaseLiteException(Status.REQUEST_TIMEOUT);
        }
    }

    private EnumSet<TDContentOptions> getContentOptions() {
        EnumSet<TDContentOptions> result = EnumSet.noneOf(TDContentOptions.class);
        if (getBooleanQuery("attachments")) {
            result.add(TDContentOptions.TDIncludeAttachments);
        }
        if (getBooleanQuery("local_seq")) {
            result.add(TDContentOptions.TDIncludeLocalSeq);
        }
        if (getBooleanQuery("conflicts")) {
            result.add(TDContentOptions.TDIncludeConflicts);
        }
        if (getBooleanQuery("revs")) {
            result.add(TDContentOptions.TDIncludeRevs);
        }
        if (getBooleanQuery("revs_info")) {
            result.add(TDContentOptions.TDIncludeRevsInfo);
        }
        return result;
    }

    private boolean getQueryOptions(QueryOptions options) {
        // http://wiki.apache.org/couchdb/HTTP_view_API#Querying_Options
        options.setSkip(getIntQuery("skip", options.getSkip()));
        options.setLimit(getIntQuery("limit", options.getLimit()));
        options.setGroupLevel(getIntQuery("group_level", options.getGroupLevel()));
        options.setDescending(getBooleanQuery("descending"));
        options.setIncludeDocs(getBooleanQuery("include_docs"));
        if (getQuery("include_deleted") != null)
            options.setAllDocsMode(Query.AllDocsMode.INCLUDE_DELETED);
        else if (getQuery("include_conflicts") != null) // nonstandard
            options.setAllDocsMode(Query.AllDocsMode.SHOW_CONFLICTS);
        else if (getQuery("only_conflicts") != null) // nonstandard
            options.setAllDocsMode(Query.AllDocsMode.ONLY_CONFLICTS);
        options.setUpdateSeq(getBooleanQuery("update_seq"));
        if (getQuery("inclusive_end") != null)
            options.setInclusiveEnd(getBooleanQuery("inclusive_end"));
        if (getQuery("inclusive_start") != null) // nonstandard
            options.setInclusiveEnd(getBooleanQuery("inclusive_start"));
        options.setPrefixMatchLevel(getIntQuery("prefix_match_level", options.getPrefixMatchLevel()));
        if (getQuery("reduce") != null) {
            options.setReduceSpecified(true);
            options.setReduce(getBooleanQuery("reduce"));
        }
        options.setGroup(getBooleanQuery("group"));

        // TODO: Stale options (ok or update_after):

        // Handle 'keys' and 'key' options:
        List<Object> keys;
        Object keysParam = getJSONQuery("keys");
        if (keysParam != null && !(keysParam instanceof List)) {
            return false;
        } else {
            keys = (List<Object>) keysParam;
        }
        if (keys == null) {
            Object key = getJSONQuery("key");
            if (key != null) {
                keys = new ArrayList<Object>();
                keys.add(key);
            }
        }
        if (keys != null) {
            options.setKeys(keys);
        } else {
            options.setStartKey(getJSONQuery("startkey"));
            options.setEndKey(getJSONQuery("endkey"));
            if (getJSONQuery("startkey_docid") != null) {
                options.setStartKeyDocId(getJSONQuery("startkey_docid").toString());
            }
            if (getJSONQuery("endkey_docid") != null) {
                options.setEndKeyDocId(getJSONQuery("endkey_docid").toString());
            }
        }
        return true;
    }

    private String getMultipartRequestType() {
        String accept = getRequestHeaderValue("Accept");
        if (accept.startsWith("multipart/")) {
            return accept;
        }
        return null;
    }

    private boolean explicitlyAcceptsType(String mimeType) {
        String accept = getRequestHeaderValue("Accept");
        return accept != null && accept.indexOf(mimeType) >= 0;
    }

    private Status openDB() {
        if (db == null) {
            return new Status(Status.INTERNAL_SERVER_ERROR);
        }
        if (!db.exists()) {
            return new Status(Status.NOT_FOUND);
        }

        try {
            db.open();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }

        return new Status(Status.OK);
    }

    private static List<String> splitPath(URL url) {
        String pathString = url.getPath();
        if (pathString.startsWith("/")) {
            pathString = pathString.substring(1);
        }
        List<String> result = new ArrayList<String>();
        //we want empty string to return empty list
        if (pathString.length() == 0) {
            return result;
        }
        for (String component : pathString.split("/")) {
            result.add(URLDecoder.decode(component));
        }
        return result;
    }

    private void sendResponse() {
        if (!responseSent) {
            responseSent = true;
            if (callbackBlock != null) {
                callbackBlock.onResponseReady();
            }
        }
    }

    // get Content-Type from URLConnection
    private String getRequestHeaderContentType() {
        String contentType = getRequestHeaderValue("Content-Type");
        if (contentType != null) {
            // remove parameter (Content-Type := type "/" subtype *[";" parameter] )
            int index = contentType.indexOf(';');
            if (index > 0)
                contentType = contentType.substring(0, index);
            contentType = contentType.trim();
        }
        return contentType;
    }

    private String getRequestHeaderValue(String paramName) {
        String value = connection.getRequestProperty(paramName);
        if (value == null)
            // From Android: http://developer.android.com/reference/java/net/URLConnection.html
            value = connection.getRequestProperty(paramName.toLowerCase());
        return value;
    }

    public void start() {
        // Refer to: http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference

        String method = connection.getRequestMethod();
        // We're going to map the request into a method call using reflection based on the method and path.
        // Accumulate the method name into the string 'message':
        if ("HEAD".equals(method)) {
            method = "GET";
        }
        String message = String.format(Locale.ENGLISH, "do_%s", method);

        // First interpret the components of the request:
        List<String> path = splitPath(connection.getURL());
        if (path == null) {
            connection.setResponseCode(Status.BAD_REQUEST);
            try {
                connection.getResponseOutputStream().close();
            } catch (IOException e) {
                Log.e(TAG, "Error closing empty output stream");
            }
            sendResponse();
            return;
        }

        int pathLen = path.size();
        if (pathLen > 0) {
            String dbName = path.get(0);
            if (dbName.startsWith("_")) {
                message += dbName;  // special root path, like /_all_dbs
            } else {
                message += "_Database";
                if (!Manager.isValidDatabaseName(dbName)) {
                    Header resHeader = connection.getResHeader();
                    if (resHeader != null) {
                        resHeader.add("Content-Type", CONTENT_TYPE_JSON);
                    }
                    Map<String, Object> result = new HashMap<String, Object>();
                    result.put("error", "Invalid database");
                    result.put("status", Status.BAD_REQUEST);
                    connection.setResponseBody(new Body(result));

                    ByteArrayInputStream bais = new ByteArrayInputStream(connection.getResponseBody().getJson());
                    connection.setResponseInputStream(bais);
                    connection.setResponseCode(Status.BAD_REQUEST);
                    try {
                        connection.getResponseOutputStream().close();
                    } catch (IOException e) {
                        Log.e(TAG, "Error closing empty output stream");
                    }
                    sendResponse();
                    return;
                } else {
                    boolean mustExist = false;
                    db = manager.getDatabase(dbName, mustExist); // NOTE: synchronized
                    if (db == null) {
                        connection.setResponseCode(Status.BAD_REQUEST);
                        try {
                            connection.getResponseOutputStream().close();
                        } catch (IOException e) {
                            Log.e(TAG, "Error closing empty output stream");
                        }
                        sendResponse();
                        return;
                    }

                }
            }
        } else {
            message += "Root";
        }

        String docID = null;
        if (db != null && pathLen > 1) {
            message = message.replaceFirst("_Database", "_Document");
            // Make sure database exists, then interpret doc name:
            Status status = openDB();
            if (!status.isSuccessful()) {
                connection.setResponseCode(status.getCode());
                try {
                    connection.getResponseOutputStream().close();
                } catch (IOException e) {
                    Log.e(TAG, "Error closing empty output stream");
                }
                sendResponse();
                return;
            }
            String name = path.get(1);
            if (!name.startsWith("_")) {
                // Regular document
                if (!Document.isValidDocumentId(name)) {
                    connection.setResponseCode(Status.BAD_REQUEST);
                    try {
                        connection.getResponseOutputStream().close();
                    } catch (IOException e) {
                        Log.e(TAG, "Error closing empty output stream");
                    }
                    sendResponse();
                    return;
                }
                docID = name;
            } else if ("_design".equals(name) || "_local".equals(name)) {
                // "_design/____" and "_local/____" are document names
                if (pathLen <= 2) {
                    connection.setResponseCode(Status.NOT_FOUND);
                    try {
                        connection.getResponseOutputStream().close();
                    } catch (IOException e) {
                        Log.e(TAG, "Error closing empty output stream");
                    }
                    sendResponse();
                    return;
                }
                docID = name + '/' + path.get(2);
                path.set(1, docID);
                path.remove(2);
                pathLen--;
            } else if (name.startsWith("_design") || name.startsWith("_local")) {
                // This is also a document, just with a URL-encoded "/"
                docID = name;
            } else if ("_session".equals(name)) {
                // There are two possible uri to get a session, /<db>/_session or /_session.
                // This is for /<db>/_session.
                message = message.replaceFirst("_Document", name);
            } else {
                // Special document name like "_all_docs":
                message += name;
                if (pathLen > 2) {
                    List<String> subList = path.subList(2, pathLen - 1);
                    StringBuilder sb = new StringBuilder();
                    Iterator<String> iter = subList.iterator();
                    while (iter.hasNext()) {
                        sb.append(iter.next());
                        if (iter.hasNext()) {
                            sb.append('/');
                        }
                    }
                    docID = sb.toString();
                }
            }
        }

        String attachmentName = null;
        if (docID != null && pathLen > 2) {
            message = message.replaceFirst("_Document", "_Attachment");
            // Interpret attachment name:
            attachmentName = path.get(2);
            if (attachmentName.startsWith("_") && docID.startsWith("_design")) {
                // Design-doc attribute like _info or _view
                message = message.replaceFirst("_Attachment", "_DesignDocument");
                docID = docID.substring(8); // strip the "_design/" prefix
                attachmentName = pathLen > 3 ? path.get(3) : null;
            } else {
                if (pathLen > 3) {
                    List<String> subList = path.subList(2, pathLen);
                    StringBuilder sb = new StringBuilder();
                    Iterator<String> iter = subList.iterator();
                    while (iter.hasNext()) {
                        sb.append(iter.next());
                        if (iter.hasNext()) {
                            //sb.append("%2F");
                            sb.append('/');
                        }
                    }
                    attachmentName = sb.toString();
                }
            }
        }

        // Send myself a message based on the components:
        Status status = null;
        try {
            Method m = Router.class.getMethod(message, Database.class, String.class, String.class);
            status = (Status) m.invoke(this, db, docID, attachmentName);
        } catch (NoSuchMethodException msme) {
            try {
                String errorMessage = String.format(Locale.ENGLISH, "Router unable to route request to %s", message);
                Log.w(TAG, errorMessage);
                // Check if there is an alternative method:
                boolean hasAltMethod = false;
                String curDoMethod = String.format(Locale.ENGLISH, "do_%s", method);
                String[] methods = {"GET", "POST", "PUT", "DELETE"};
                for (String aMethod : methods) {
                    if (!aMethod.equals(method)) {
                        String altDoMethod = String.format(Locale.ENGLISH, "do_%s", aMethod);
                        String altMessage = message.replaceAll(curDoMethod, altDoMethod);
                        try {
                            Method altMethod = Router.class.getMethod(altMessage,
                                    Database.class, String.class, String.class);
                            hasAltMethod = true;
                            break;
                        } catch (Exception ex) {
                            // go next
                        }
                    }
                }
                Method m = Router.class.getMethod(
                        hasAltMethod ? "do_METHOD_NOT_ALLOWED" : "do_UNKNOWN",
                        Database.class, String.class, String.class);
                status = (Status) m.invoke(this, db, docID, attachmentName);
            } catch (Exception e) {
                //default status is internal server error
                Log.e(TAG, "Router attempted do_UNKNWON fallback, but that threw an exception", e);
                status = new Status(Status.NOT_FOUND);
                Map<String, Object> result = new HashMap<String, Object>();
                result.put("status", status.getHTTPCode());
                result.put("error", status.getHTTPMessage());
                result.put("reason", "Router unable to route request");
                connection.setResponseBody(new Body(result));
            }
        } catch (Exception e) {
            String errorMessage = "Router unable to route request to " + message;
            Log.w(TAG, errorMessage, e);
            Map<String, Object> result = new HashMap<String, Object>();
            if (e.getCause() != null && e.getCause() instanceof CouchbaseLiteException) {
                status = ((CouchbaseLiteException) e.getCause()).getCBLStatus();
                result.put("status", status.getHTTPCode());
                result.put("error", status.getHTTPMessage());
                result.put("reason", errorMessage + e.getCause().toString());
            } else {
                status = new Status(Status.NOT_FOUND);
                result.put("status", status.getHTTPCode());
                result.put("error", status.getHTTPMessage());
                result.put("reason", errorMessage + e.toString());
            }
            connection.setResponseBody(new Body(result));
        }

        // If response is ready (nonzero status), tell my client about it:
        if (status.getCode() != 0) {
            // NOTE: processRequestRanges() is not implemented for CBL Java Core

            // Configure response headers:
            status = sendResponseHeaders(status);

            connection.setResponseCode(status.getCode());

            if (status.isSuccessful() &&
                connection.getResponseBody() == null &&
                connection.getHeaderField("Content-Type") == null &&
                dontOverwriteBody == false) {
                connection.setResponseBody(new Body("{\"ok\":true}".getBytes()));
            }

            if (!status.isSuccessful() && connection.getResponseBody() == null) {
                Map<String, Object> result = new HashMap<String, Object>();
                result.put("status", status.getCode());
                result.put("error", status.getHTTPMessage());
                connection.setResponseBody(new Body(result));
                connection.getResHeader().add("Content-Type", CONTENT_TYPE_JSON);
            }

            setResponse();
            sendResponse();
        } else {
            // NOTE code == 0
            waiting = true;
        }

        if (waiting && db != null) {
            Log.v(TAG, "waiting=true & db!=null: call Database.addDatabaseListener()");
            db.addDatabaseListener(this);
        }
    }

    /**
     * implementation of Database.DatabaseListener
     */
    @Override
    public void databaseClosing() {
        dbClosing();
    }

    private void dbClosing() {
        Log.d(TAG, "Database closing! Returning error 500");
        Status status = new Status(Status.INTERNAL_SERVER_ERROR);
        status = sendResponseHeaders(status);
        connection.setResponseCode(status.getCode());
        setResponse();
        sendResponse();
    }

    public void stop() {
        stopHeartbeat();
        callbackBlock = null;
        if (db != null) {
            db.removeChangeListener(this);
            db.removeDatabaseListener(this);
        }
    }

    public Status do_UNKNOWN(Database db, String docID, String attachmentName) {
        return new Status(Status.NOT_FOUND);
    }

    public Status do_METHOD_NOT_ALLOWED(Database db, String docID, String attachmentName) {
        return new Status(Status.METHOD_NOT_ALLOWED);
    }

    private void setResponse() {
        if (connection.getResponseBody() != null) {
            ByteArrayInputStream bais = new ByteArrayInputStream(connection.getResponseBody().getJson());
            connection.setResponseInputStream(bais);
        } else {
            try {
                connection.getResponseOutputStream().close();
            } catch (IOException e) {
                Log.e(TAG, "Error closing empty output stream");
            }
        }
    }

    /**
     * in CBL_Router.m
     * - (void) sendResponseHeaders
     */
    private Status sendResponseHeaders(Status status) {
        // NOTE: Line 572-574 of CBL_Router.m is not in CBL Java Core
        //       This check is in sendResponse();
        connection.getResHeader().add("Server",
                String.format(Locale.ENGLISH, "Couchbase Lite %s", getVersionString()));

        // When response body is not null, we can assume that the body is JSON:
        boolean hasJSONBody = connection.getResponseBody() != null;
        String contentType = hasJSONBody ? CONTENT_TYPE_JSON : null;

        // Check for a mismatch between the Accept request header and the response type:
        String accept = getRequestHeaderValue("Accept");
        if (accept != null && accept.indexOf("*/*") < 0) {
            String baseContentType = connection.getBaseContentType();
            if (baseContentType == null)
                baseContentType = contentType;
            if (baseContentType != null && baseContentType.indexOf(accept) < 0) {
                Log.w(TAG, "Error 406: Can't satisfy request Accept: %s (Content-Type = %s)",
                        accept, contentType);

                // Reset response body:
                connection.setResponseBody(null);

                status = new Status(Status.NOT_ACCEPTABLE);
                return status;
            }
        }

        if (contentType != null) {
            Header resHeader = connection.getResHeader();
            if (resHeader != null && resHeader.get("Content-Type") == null)
                resHeader.add("Content-Type", contentType);
            else
                Log.d(TAG, "Cannot add Content-Type header because getResHeader() returned null");
        }

        // NOTE: Line 596-607 of CBL_Router.m is not in CBL Java Core
        return status;
    }

    /**
     * Router+Handlers
     */

    private void setResponseLocation(URL url) {
        String location = url.getPath();
        String query = url.getQuery();
        if (query != null) {
            int startOfQuery = location.indexOf(query);
            if (startOfQuery > 0) {
                location = location.substring(0, startOfQuery);
            }
        }
        connection.getResHeader().add("Location", location);
    }

    /**
     * SERVER REQUESTS: *
     */

    public Status do_GETRoot(Database _db, String _docID, String _attachmentName) {
        Map<String, Object> info = new HashMap<String, Object>();
        info.put("CBLite", "Welcome");
        info.put("couchdb", "Welcome"); // for compatibility
        info.put("version", getVersionString());
        connection.setResponseBody(new Body(info));
        return new Status(Status.OK);
    }

    public Status do_GET_all_dbs(Database _db, String _docID, String _attachmentName) {
        List<String> dbs = manager.getAllDatabaseNames();
        connection.setResponseBody(new Body(dbs));
        return new Status(Status.OK);
    }

    public Status do_GET_session(Database _db, String _docID, String _attachmentName) {
        // Send back an "Admin Party"-like response
        Map<String, Object> session = new HashMap<String, Object>();
        Map<String, Object> userCtx = new HashMap<String, Object>();
        String[] roles = {"_admin"};
        session.put("ok", true);
        userCtx.put("name", null);
        userCtx.put("roles", roles);
        session.put("userCtx", userCtx);
        connection.setResponseBody(new Body(session));
        return new Status(Status.OK);
    }

    public Status do_POST_replicate(Database _db, String _docID, String _attachmentName) {

        Replication replicator;

        // Extract the parameters from the JSON request body:
        // http://wiki.apache.org/couchdb/Replication
        Map<String, Object> body = null;
        try {
            body = getBodyAsDictionary();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }

        try {
            // NOTE: replicator instance is created per request. not access shared instances
            replicator = manager.getReplicator(body);
        } catch (CouchbaseLiteException e) {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("error", e.toString());
            connection.setResponseBody(new Body(result));
            return e.getCBLStatus();
        }

        Boolean cancelBoolean = (Boolean) body.get("cancel");
        boolean cancel = (cancelBoolean != null && cancelBoolean.booleanValue());

        if (!cancel) {

            if (!replicator.isRunning()) {

                final CountDownLatch replicationStarted = new CountDownLatch(1);
                replicator.addChangeListener(new Replication.ChangeListener() {
                    @Override
                    public void changed(Replication.ChangeEvent event) {
                        if (event.getTransition() != null &&
                                event.getTransition().getDestination() == ReplicationState.RUNNING) {
                            replicationStarted.countDown();
                        }
                    }
                });

                if (!replicator.isContinuous()) {
                    replicator.addChangeListener(new Replication.ChangeListener() {
                        @Override
                        public void changed(Replication.ChangeEvent event) {
                            if (event.getTransition() != null &&
                                    event.getTransition().getDestination() == ReplicationState.STOPPED) {
                                Status status = new Status(Status.OK);
                                status = sendResponseHeaders(status);
                                connection.setResponseCode(status.getCode());
                                Map<String, Object> result = new HashMap<String, Object>();
                                result.put("session_id", event.getSource().getSessionID());
                                connection.setResponseBody(new Body(result));

                                setResponse();
                                sendResponse();
                            }
                        }
                    });
                }

                replicator.start();

                // wait for replication to start, otherwise replicator.getSessionId() will return null
                try {
                    replicationStarted.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }

            if (replicator.isContinuous()) {
                Map<String, Object> result = new HashMap<String, Object>();
                result.put("session_id", replicator.getSessionID());
                connection.setResponseBody(new Body(result));
                return new Status(Status.OK);
            } else {
                return new Status(0);
            }
        } else {
            // Cancel replication:
            replicator.stop();
            return new Status(Status.OK);
        }
    }

    public Status do_GET_uuids(Database _db, String _docID, String _attachmentName) {
        int count = Math.min(1000, getIntQuery("count", 1));
        List<String> uuids = new ArrayList<String>(count);
        for (int i = 0; i < count; i++) {
            uuids.add(Misc.CreateUUID());
        }
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("uuids", uuids);
        connection.setResponseBody(new Body(result));
        return new Status(Status.OK);
    }

    /**
     * TODO: CBL Java Core codes are out of sync with CBL iOS. Need to catch up CBL iOS
     */
    public Status do_GET_active_tasks(Database _db, String _docID, String _attachmentName) {
        // http://wiki.apache.org/couchdb/HttpGetActiveTasks

        String feed = getQuery("feed");
        longpoll = "longpoll".equals(feed);
        boolean continuous = !longpoll && "continuous".equals(feed);

        String session_id = getQuery("session_id");

        ChangeListener listener = new ChangeListener() {

            ChangeListener self = this;

            @Override
            public void changed(ChangeEvent event) {

                Map<String, Object> activity = getActivity(event.getSource());

                // NOTE: Followings are not supported by iOS. We might remove them in the future.
                if (event.getTransition() != null) {
                    // this adds data to the response based on the trigger.
                    activity.put("transition_source", event.getTransition().getSource());
                    activity.put("transition_destination", event.getTransition().getDestination());
                    activity.put("trigger", event.getTransition().getTrigger());
                    Log.d(TAG,
                            "do_GET_active_tasks Transition [" + event.getTransition().getTrigger() +
                                    "] Source:" + event.getTransition().getSource() +
                                    ", Destination:" + event.getTransition().getDestination());
                }

                if (longpoll) {
                    Log.w(TAG, "Router: Sending longpoll replication response");
                    sendResponse();
                    if (callbackBlock != null) {
                        byte[] data = null;
                        try {
                            data = Manager.getObjectMapper().writeValueAsBytes(activity);
                        } catch (Exception e) {
                            Log.w(TAG, "Error serializing JSON", e);
                        }
                        OutputStream os = connection.getResponseOutputStream();
                        try {
                            os.write(data);
                            os.close();
                        } catch (IOException e) {
                            Log.e(TAG, "IOException writing to internal streams", e);
                        }
                    }
                    //remove this change listener because a new one will be added when this responds
                    event.getSource().removeChangeListener(self);
                } else {
                    Log.w(TAG, "Router: Sending continous replication change chunk");
                    sendContinuousReplicationChanges(activity);
                }
            }
        };

        List<Map<String, Object>> activities = new ArrayList<Map<String, Object>>();
        for (Database db : manager.allOpenDatabases()) {
            List<Replication> activeReplicators = db.getActiveReplications();
            if (activeReplicators != null) {
                for (Replication replicator : activeReplicators) {
                    if (replicator.isRunning()) {
                        Map<String, Object> activity = getActivity(replicator);

                        if (session_id != null) {
                            if (replicator.getSessionID().equals(session_id)) {
                                activities.add(activity);
                            }
                        } else {
                            activities.add(activity);
                        }

                        if (continuous || longpoll) {
                            if (session_id != null) {
                                if (replicator.getSessionID().equals(session_id)) {
                                    replicator.addChangeListener(listener);
                                }
                            } else {
                                replicator.addChangeListener(listener);
                            }
                        }
                    }
                }
            }
        }

        if (continuous || longpoll) {
            connection.setChunked(true);
            connection.setResponseCode(Status.OK);
            sendResponse();
            if (continuous && !activities.isEmpty()) {
                for (Map<String, Object> activity : activities) {
                    sendContinuousReplicationChanges(activity);
                }
            }

            // Don't close connection; more data to come
            return new Status(0);
        } else {
            connection.setResponseBody(new Body(activities));
            return new Status(Status.OK);
        }
    }

    /**
     * TODO: To be compatible with CBL iOS, this method should move to Replicator.activeTaskInfo().
     * TODO: Reference: - (NSDictionary*) activeTaskInfo in CBL_Replicator.m
     */
    private static Map<String, Object> getActivity(Replication replicator) {
        // For schema, see http://wiki.apache.org/couchdb/HttpGetActiveTasks
        Map<String, Object> activity = new HashMap<String, Object>();

        String source = replicator.getRemoteUrl().toExternalForm();
        String target = replicator.getLocalDatabase().getName();

        if (!replicator.isPull()) {
            String tmp = source;
            source = target;
            target = tmp;
        }
        int processed = replicator.getCompletedChangesCount();
        int total = replicator.getChangesCount();
        String status = String.format(Locale.ENGLISH, "Processed %d / %d changes", processed, total);
        if (!replicator.getStatus().equals(ReplicationStatus.REPLICATION_ACTIVE)) {
            //These values match the values for IOS.
            if (replicator.getStatus().equals(ReplicationStatus.REPLICATION_IDLE)) {
                status = "Idle";     // nonstandard
            } else if (replicator.getStatus().equals(ReplicationStatus.REPLICATION_STOPPED)) {
                status = "Stopped";
            } else if (replicator.getStatus().equals(ReplicationStatus.REPLICATION_OFFLINE)) {
                status = "Offline";  // nonstandard
            }
        }
        int progress = (total > 0) ? Math.round(100 * processed / (float) total) : 0;

        activity.put("type", "Replication");
        activity.put("task", replicator.getSessionID());
        activity.put("source", source);
        activity.put("target", target);
        activity.put("status", status);
        activity.put("progress", progress);
        activity.put("continuous", replicator.isContinuous());

        //NOTE: Need to support "x_active_requests"

        if (replicator.getLastError() != null) {
            String msg = String.format(Locale.ENGLISH, "Replicator error: %s.  Repl: %s.  Source: %s, Target: %s",
                    replicator.getLastError(), replicator, source, target);
            Log.w(TAG, msg);
            Throwable error = replicator.getLastError();
            int statusCode = 400;
            if (error instanceof RemoteRequestResponseException) {
                statusCode = ((RemoteRequestResponseException) error).getCode();
            }
            Object[] errorObjects = new Object[]{statusCode, replicator.getLastError().toString()};
            activity.put("error", errorObjects);
        } else {
            // NOTE: Following two parameters: CBL iOS does not support. We might remove them in the future.
            activity.put("change_count", total);
            activity.put("completed_change_count", processed);
        }

        return activity;
    }

    /**
     * Send a JSON object followed by a newline without closing the connection.
     * Used by the continuous mode of _changes and _active_tasks.
     * <p/>
     * TODO: CBL iOS supports EventSourceFeed in addition to longpoll and continuous.
     * TODO: Need to catch up CBL iOS:
     * - (void) sendContinuousLine: (NSDictionary*)changeDict in CBL_Router+Handlers.m
     */
    private void sendContinuousReplicationChanges(Map<String, Object> activity) {
        try {
            String jsonString = Manager.getObjectMapper().writeValueAsString(activity);
            if (callbackBlock != null) {
                byte[] json = (jsonString + '\n').getBytes();
                OutputStream os = connection.getResponseOutputStream();
                try {
                    os.write(json);
                    os.flush();
                } catch (Exception e) {
                    Log.e(TAG, "IOException writing to internal streams", e);
                }
            }
        } catch (Exception e) {
            Log.w("Unable to serialize change to JSON", e);
        }
    }

    /**
     * DATABASE REQUESTS: *
     */

    public Status do_GET_Database(Database _db, String _docID, String _attachmentName) {
        // http://wiki.apache.org/couchdb/HTTP_database_API#Database_Information
        Status status = openDB();
        if (!status.isSuccessful()) {
            return status;
        }
        // NOTE: all methods are read operation. not necessary to be synchronized
        int num_docs = db.getDocumentCount();
        long update_seq = db.getLastSequenceNumber();
        long instanceStartTimeMicroseconds = db.getStartTime() * 1000;
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("db_name", db.getName());
        result.put("db_uuid", db.publicUUID());
        result.put("doc_count", num_docs);
        result.put("update_seq", update_seq);
        result.put("disk_size", db.totalDataSize());
        result.put("instance_start_time", instanceStartTimeMicroseconds);

        connection.setResponseBody(new Body(result));
        return new Status(Status.OK);
    }

    public Status do_PUT_Database(Database _db, String _docID, String _attachmentName) {
        if (db.exists()) {
            return new Status(Status.DUPLICATE);
        }

        try {
            // note: synchronized
            db.open();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }

        setResponseLocation(connection.getURL());
        return new Status(Status.CREATED);
    }

    public Status do_DELETE_Database(Database _db, String _docID, String _attachmentName)
            throws CouchbaseLiteException {
        if (getQuery("rev") != null) {
            return new Status(Status.BAD_REQUEST);
            // CouchDB checks for this; probably meant to be a document deletion
        }
        db.delete();
        return new Status(Status.OK);
    }

    /**
     * This is a hack to deal with the fact that there is currently no custom
     * serializer for QueryRow.  Instead, just convert everything to generic Maps.
     */
    private static void convertCBLQueryRowsToMaps(Map<String, Object> allDocsResult) {
        List<Map<String, Object>> rowsAsMaps = new ArrayList<Map<String, Object>>();
        List<QueryRow> rows = (List<QueryRow>) allDocsResult.get("rows");
        if (rows != null) {
            for (QueryRow row : rows) {
                rowsAsMaps.add(row.asJSONDictionary());
            }
        }
        allDocsResult.put("rows", rowsAsMaps);
    }

    public Status do_POST_Database(Database _db, String _docID, String _attachmentName) {
        Status status = openDB();
        if (!status.isSuccessful()) {
            return status;
        }
        Map<String, Object> body;
        try {
            body = getBodyAsDictionary();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }
        return update(db, null, new Body(body), false);
    }

    public Status do_GET_Document_all_docs(Database _db, String _docID, String _attachmentName)
            throws CouchbaseLiteException {
        QueryOptions options = new QueryOptions();
        if (!getQueryOptions(options)) {
            return new Status(Status.BAD_REQUEST);
        }
        Map<String, Object> result = db.getAllDocs(options);
        convertCBLQueryRowsToMaps(result);
        if (result == null) {
            return new Status(Status.INTERNAL_SERVER_ERROR);
        }
        connection.setResponseBody(new Body(result));
        return new Status(Status.OK);
    }

    public Status do_POST_Document_all_docs(Database _db, String _docID, String _attachmentName)
            throws CouchbaseLiteException {
        QueryOptions options = new QueryOptions();
        if (!getQueryOptions(options)) {
            return new Status(Status.BAD_REQUEST);
        }

        Map<String, Object> body = getBodyAsDictionary();
        if (body == null) {
            return new Status(Status.BAD_REQUEST);
        }

        List<Object> keys = (List<Object>) body.get("keys");
        options.setKeys(keys);

        Map<String, Object> result = null;
        result = db.getAllDocs(options);
        convertCBLQueryRowsToMaps(result);

        if (result == null) {
            return new Status(Status.INTERNAL_SERVER_ERROR);
        }
        connection.setResponseBody(new Body(result));
        return new Status(Status.OK);
    }

    public Status do_POST_facebook_token(Database _db, String _docID, String _attachmentName) {
        Map<String, Object> body;
        try {
            body = getBodyAsDictionary();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }

        String email = (String) body.get("email");
        String remoteUrl = (String) body.get("remote_url");
        String accessToken = (String) body.get("access_token");
        if (email != null && remoteUrl != null && accessToken != null) {
            URL siteUrl;
            try {
                siteUrl = new URL(remoteUrl);
            } catch (MalformedURLException e) {
                Map<String, Object> result = new HashMap<String, Object>();
                result.put("error", "invalid remote_url: " + e.getLocalizedMessage());
                connection.setResponseBody(new Body(result));
                return new Status(Status.BAD_REQUEST);
            }

            FacebookAuthorizer.registerToken(accessToken, email, siteUrl);

            Map<String, Object> result = new HashMap<String, Object>();
            result.put("ok", "registered");
            connection.setResponseBody(new Body(result));
            return new Status(Status.OK);
        } else {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("error", "required fields: access_token, email, remote_url");
            connection.setResponseBody(new Body(result));
            return new Status(Status.BAD_REQUEST);
        }
    }

    public Status do_POST_persona_assertion(Database _db, String _docID, String _attachmentName) {
        Map<String, Object> body;
        try {
            body = getBodyAsDictionary();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }

        String assertion = (String) body.get("assertion");

        if (assertion == null) {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("error", "required fields: assertion");
            connection.setResponseBody(new Body(result));
            return new Status(Status.BAD_REQUEST);
        }

        try {
            String email = PersonaAuthorizer.registerAssertion(assertion);

            Map<String, Object> result = new HashMap<String, Object>();
            result.put("ok", "registered");
            result.put("email", email);

            connection.setResponseBody(new Body(result));
            return new Status(Status.OK);

        } catch (Exception e) {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("error", "error registering persona assertion: " + e.getLocalizedMessage());
            connection.setResponseBody(new Body(result));
            return new Status(Status.BAD_REQUEST);
        }


    }

    public Status do_POST_Document_bulk_docs(Database _db, String _docID, String _attachmentName) {
        // http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API
        Map<String, Object> body;
        try {
            body = getBodyAsDictionary();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }
        final List<Map<String, Object>> docs = (List<Map<String, Object>>) body.get("docs");

        final boolean noNewEdits = getBooleanValueFromBody("new_edits", body, true) == false;
        final boolean allOrNothing = getBooleanValueFromBody("all_or_nothing", body, false);

        final Status status = new Status(Status.OK);
        final List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        // Transaction provide synchronized feature by SQLiteDatabase.
        // In the transaction block, should not use `synchronized` block
        boolean ret = db.getStore().runInTransaction(new TransactionalTask() {
            @Override
            public boolean run() {
                boolean ok = false;
                try {
                    for (Map<String, Object> doc : docs) {
                        String docID = (String) doc.get("_id");
                        RevisionInternal rev = null;

                        Body docBody = new Body(doc);
                        if (noNewEdits) {
                            rev = new RevisionInternal(docBody);
                            if (rev.getRevID() == null || rev.getDocID() == null ||
                                    !rev.getDocID().equals(docID)) {
                                status.setCode(Status.BAD_REQUEST);
                            } else {
                                List<String> history = Database.parseCouchDBRevisionHistory(doc);
                                db.forceInsert(rev, history, source);
                            }
                        } else {
                            Status outStatus = new Status();
                            rev = update(db, docID, docBody, false, allOrNothing, outStatus);
                            status.setCode(outStatus.getCode());
                        }
                        Map<String, Object> result = null;
                        if (status.isSuccessful()) {
                            result = new HashMap<String, Object>();
                            result.put("ok", true);
                            result.put("id", docID);
                            if (rev != null) {
                                result.put("rev", rev.getRevID());
                            }
                        } else if (allOrNothing) {
                            return false;
                        } else if (status.getCode() == Status.FORBIDDEN) {
                            result = new HashMap<String, Object>();
                            result.put("error", "validation failed");
                            result.put("id", docID);
                        } else if (status.getCode() == Status.CONFLICT) {
                            result = new HashMap<String, Object>();
                            result.put("error", "conflict");
                            result.put("id", docID);
                        } else {
                            //return status;  // abort the whole thing if something goes badly wrong
                            return false;
                        }
                        if (result != null) {
                            results.add(result);
                        }
                    }
                    Log.w(TAG, "%s finished inserting %d revisions in bulk", this, docs.size());
                    ok = true;
                } catch (Exception e) {
                    Log.e(TAG, "%s: Exception inserting revisions in bulk", e, this);
                } finally {
                    return ok;
                }
            }
        });

        if (ret) {
            connection.setResponseBody(new Body(results));
            return new Status(Status.CREATED);
        } else {
            return status;
        }
    }

    public Status do_POST_Document_revs_diff(Database _db, String _docID, String _attachmentName) {
        // http://wiki.apache.org/couchdb/HttpPostRevsDiff
        // Collect all of the input doc/revision IDs as TDRevisions:
        RevisionList revs = new RevisionList();
        Map<String, Object> body;
        try {
            body = getBodyAsDictionary();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }

        for (String docID : body.keySet()) {
            List<String> revIDs = (List<String>) body.get(docID);
            for (String revID : revIDs) {
                RevisionInternal rev = new RevisionInternal(docID, revID, false);
                revs.add(rev);
            }
        }

        // Look them up, removing the existing ones from revs:
        try {
            // query db only, not necessary to be syncrhonized
            db.findMissingRevisions(revs);
        } catch (SQLException e) {
            Log.e(TAG, "Exception", e);
            return new Status(Status.DB_ERROR);
        }

        // Return the missing revs in a somewhat different format:
        Map<String, Object> diffs = new HashMap<String, Object>();
        for (RevisionInternal rev : revs) {
            String docID = rev.getDocID();

            List<String> missingRevs = null;
            Map<String, Object> idObj = (Map<String, Object>) diffs.get(docID);
            if (idObj != null) {
                missingRevs = (List<String>) idObj.get("missing");
            } else {
                idObj = new HashMap<String, Object>();
            }

            if (missingRevs == null) {
                missingRevs = new ArrayList<String>();
                idObj.put("missing", missingRevs);
                diffs.put(docID, idObj);
            }
            missingRevs.add(rev.getRevID());
        }

        // FIXME add support for possible_ancestors

        connection.setResponseBody(new Body(diffs));
        return new Status(Status.OK);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public Status do_POST_Document_compact(Database _db, String _docID, String _attachmentName) {
        Status status = new Status(Status.OK);
        try {
            // Make Database.compact() thread-safe
            _db.compact();
        } catch (CouchbaseLiteException e) {
            status = e.getCBLStatus();
        }

        if (status.getCode() < 300) {
            Status outStatus = new Status();
            outStatus.setCode(202);    // CouchDB returns 202 'cause it's an async operation
            return outStatus;
        } else {
            return status;
        }
    }

    public Status do_POST_Document_purge(Database _db, String ignored1, String ignored2) {
        Map<String, Object> body;
        try {
            body = getBodyAsDictionary();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }

        // convert from Map<String,Object> -> Map<String, List<String>> - is there a cleaner way?
        final Map<String, List<String>> docsToRevs = new HashMap<String, List<String>>();
        for (String key : body.keySet()) {
            Object val = body.get(key);
            if (val instanceof List) {
                docsToRevs.put(key, (List<String>) val);
            }
        }

        final List<Map<String, Object>> asyncApiCallResponse = new ArrayList<Map<String, Object>>();

        // this is run in an async db task to fix the following race condition
        // found in issue #167 (https://github.com/couchbase/couchbase-lite-android/issues/167)
        // replicator thread: call db.loadRevisionBody for doc1
        // liteserv thread: call db.purge on doc1
        // replicator thread: call db.getRevisionHistory for doc1, which returns empty history since it was purged
        Future future = db.runAsync(new AsyncTask() {
            @Override
            public void run(Database database) {
                // purgeRevisions uses transaction internally.
                Map<String, Object> purgedRevisions = db.purgeRevisions(docsToRevs);
                asyncApiCallResponse.add(purgedRevisions);
            }
        });
        try {
            future.get(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Log.e(TAG, "Exception waiting for future", e);
            return new Status(Status.INTERNAL_SERVER_ERROR);
        } catch (ExecutionException e) {
            Log.e(TAG, "Exception waiting for future", e);
            return new Status(Status.INTERNAL_SERVER_ERROR);
        } catch (TimeoutException e) {
            Log.e(TAG, "Exception waiting for future", e);
            return new Status(Status.INTERNAL_SERVER_ERROR);
        }

        Map<String, Object> purgedRevisions = asyncApiCallResponse.get(0);
        Map<String, Object> responseMap = new HashMap<String, Object>();
        responseMap.put("purged", purgedRevisions);
        Body responseBody = new Body(responseMap);
        connection.setResponseBody(responseBody);
        return new Status(Status.OK);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public Status do_POST_Document_ensure_full_commit(Database _db, String _docID, String _attachmentName) {
        return new Status(Status.OK);
    }

    /**
     * CHANGES: *
     */

    private Map<String, Object> changesDictForRevision(RevisionInternal rev) {
        Map<String, Object> changesDict = new HashMap<String, Object>();
        changesDict.put("rev", rev.getRevID());

        List<Map<String, Object>> changes = new ArrayList<Map<String, Object>>();
        changes.add(changesDict);

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("seq", rev.getSequence());
        result.put("id", rev.getDocID());
        result.put("changes", changes);
        if (rev.isDeleted()) {
            result.put("deleted", true);
        }
        if (changesIncludesDocs) {
            result.put("doc", rev.getProperties());
        }
        return result;
    }

    private Map<String, Object> responseBodyForChanges(List<RevisionInternal> changes, long since) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        for (RevisionInternal rev : changes) {
            Map<String, Object> changeDict = changesDictForRevision(rev);
            results.add(changeDict);
        }
        if (changes.size() > 0) {
            since = changes.get(changes.size() - 1).getSequence();
        }
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("results", results);
        result.put("last_seq", since);
        return result;
    }

    private Map<String, Object> responseBodyForChangesWithConflicts(List<RevisionInternal> changes, long since) {
        // Assumes the changes are grouped by docID so that conflicts will be adjacent.
        List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>();
        String lastDocID = null;
        Map<String, Object> lastEntry = null;
        for (RevisionInternal rev : changes) {
            String docID = rev.getDocID();
            if (docID.equals(lastDocID)) {
                Map<String, Object> changesDict = new HashMap<String, Object>();
                changesDict.put("rev", rev.getRevID());
                List<Map<String, Object>> inchanges = (List<Map<String, Object>>) lastEntry.get("changes");
                inchanges.add(changesDict);
            } else {
                lastEntry = changesDictForRevision(rev);
                entries.add(lastEntry);
                lastDocID = docID;
            }
        }
        // After collecting revisions, sort by sequence:
        Collections.sort(entries, new Comparator<Map<String, Object>>() {
            public int compare(Map<String, Object> e1, Map<String, Object> e2) {
                return Misc.SequenceCompare((Long) e1.get("seq"), (Long) e2.get("seq"));
            }
        });

        Long lastSeq;
        if (entries.size() == 0) {
            lastSeq = since;
        } else {
            lastSeq = (Long) entries.get(entries.size() - 1).get("seq");
            if (lastSeq == null) {
                lastSeq = since;
            }
        }

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("results", entries);
        result.put("last_seq", lastSeq);
        return result;
    }

    private void sendContinuousChange(RevisionInternal rev) {
        Map<String, Object> changeDict = changesDictForRevision(rev);
        try {
            String jsonString = Manager.getObjectMapper().writeValueAsString(changeDict);
            if (callbackBlock != null) {
                byte[] json = (jsonString + '\n').getBytes();
                OutputStream os = connection.getResponseOutputStream();
                try {
                    os.write(json);
                    os.flush();
                } catch (Exception e) {
                    Log.e(TAG, "IOException writing to internal streams", e);
                }
            }
        } catch (Exception e) {
            Log.w("Unable to serialize change to JSON", e);
        }
    }

    /**
     * Implementation of ChangeListener
     */
    @Override
    public void changed(Database.ChangeEvent event) {
        List<RevisionInternal> revs = new ArrayList<RevisionInternal>();
        List<DocumentChange> changes = event.getChanges();
        for (DocumentChange change : changes) {
            RevisionInternal rev = change.getAddedRevision();
            if (rev == null)
                continue;
            String winningRevID = change.getWinningRevisionID();
            if (!this.changesIncludesConflicts) {
                if (winningRevID == null)
                    continue; // // this change doesn't affect the winning rev ID, no need to send it
                else if (!winningRevID.equals(rev.getRevID())) {
                    // This rev made a _different_ rev current, so substitute that one.
                    // We need to emit the current sequence # in the feed, so put it in the rev.
                    // This isn't correct internally (this is an old rev so it has an older sequence)
                    // but consumers of the _changes feed don't care about the internal state.
                    RevisionInternal mRev = db.getDocument(rev.getDocID(), winningRevID, changesIncludesDocs);
                    mRev.setSequence(rev.getSequence());
                    rev = mRev;
                }
            }

            if (!event.getSource().runFilter(changesFilter, changesFilterParams, rev))
                continue;

            if (longpoll) {
                revs.add(rev);
            } else {
                Log.i(TAG, "Router: Sending continous change chunk");
                sendContinuousChange(rev);
            }
        }

        if (longpoll && revs.size() > 0) {
            // in case of /_changes with longpoll, the connection is critical section
            // when case multiple threads write a doc simultaneously.
            synchronized (databaseChangesLongpollLock) {
                Log.i(TAG, "Router: Sending longpoll response: START");
                sendResponse();
                OutputStream os = connection.getResponseOutputStream();
                try {
                    Map<String, Object> body = responseBodyForChanges(revs, 0);
                    if (callbackBlock != null) {
                        byte[] data = null;
                        try {
                            data = Manager.getObjectMapper().writeValueAsBytes(body);
                        } catch (Exception e) {
                            Log.w(TAG, "Error serializing JSON", e);
                        }
                        os.write(data);
                        os.flush();
                    }
                } catch (IOException e) {
                    // NOTE: Under multi-threads environment, OutputStream could be already closed
                    // by other thread. Because multiple Database write operations
                    // from multiple threads cause `changed(ChangeEvent)` callbacks
                    // from multiple threads simultaneously because `changed` is fired
                    // at out of transaction after endTransaction(). So this is ignorable error.
                    // So print warning message, and exit from method.
                    // Stacktrace should not be printed, it confuses developer.
                    // https://github.com/couchbase/couchbase-lite-java-core/issues/1043
                    Log.w(TAG, "IOException writing to internal streams: " + e.getMessage());
                } finally {
                    try {
                        if (os != null) {
                            os.close();
                        }
                    } catch (IOException e) {
                        Log.w(TAG, "Failed to close connection: " + e.getMessage());
                    }
                }
                Log.i(TAG, "Router: Sending longpoll response: END");
            }
        }
    }

    public Status do_GET_Document_changes(Database _db, String docID, String _attachmentName) {
        return doChanges(_db);
    }

    public Status do_POST_Document_changes(Database _db, String docID, String _attachmentName) {
        // Merge the properties from the JSON request body into the URL queries.
        // Note that values in _queries have to be NSStrings or the parsing code will break!
        Map<String, Object> body;
        try {
            body = getBodyAsDictionary();
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }
        Iterator<String> keys = body.keySet().iterator();
        while (keys.hasNext()) {
            if (getQueries() == null)
                this.queries = new HashMap<String, String>();
            String key = keys.next();
            Object value = body.get(key);
            if (key != null && value != null)
                getQueries().put(key, value.toString());
        }

        return doChanges(_db);
    }

    private Status doChanges(Database db) {
        // http://docs.couchdb.org/en/latest/api/database/changes.html
        // http://wiki.apache.org/couchdb/HTTP_database_API#Changes
        changesIncludesDocs = getBooleanQuery("include_docs");
        String style = getQuery("style");
        if (style != null && "all_docs".equals(style))
            changesIncludesConflicts = true;

        ChangesOptions options = new ChangesOptions();
        options.setIncludeDocs(changesIncludesDocs);
        options.setIncludeConflicts(changesIncludesConflicts);
        options.setSortBySequence(!options.isIncludeConflicts());
        // TODO: descending option is not supported by ChangesOptions
        options.setLimit(getIntQuery("limit", options.getLimit()));

        int since = getIntQuery("since", 0);

        String filterName = getQuery("filter");
        if (filterName != null) {
            changesFilter = db.getFilter(filterName);
            if (changesFilter == null) {
                return new Status(Status.NOT_FOUND);
            }
            changesFilterParams = new HashMap<String, Object>(queries);
            Log.v(TAG, "Filter params=" + changesFilterParams);
        }

        // changesSince() is query only. not required synchronized
        RevisionList changes = db.changesSince(since, options, changesFilter, changesFilterParams);

        if (changes == null) {
            return new Status(Status.INTERNAL_SERVER_ERROR);
        }

        String feed = getQuery("feed");
        longpoll = "longpoll".equals(feed);
        boolean continuous = !longpoll && "continuous".equals(feed);

        if (continuous || (longpoll && changes.size() == 0)) {
            connection.setChunked(true);
            connection.setResponseCode(Status.OK);
            sendResponse();
            if (continuous) {
                for (RevisionInternal rev : changes) {
                    sendContinuousChange(rev);
                }
            }
            db.addChangeListener(this);

            // heartbeat
            String heartbeatParam = getQuery("heartbeat");
            if (heartbeatParam != null) {
                long heartbeat = 0;
                try {
                    heartbeat = (long) Double.parseDouble(heartbeatParam);
                } catch (Exception e) {
                    return new Status(Status.BAD_REQUEST);
                }
                if (heartbeat <= 0)
                    return new Status(Status.BAD_REQUEST);
                else if (heartbeat < MIN_HEARTBEAT)
                    heartbeat = MIN_HEARTBEAT;
                startHeartbeat(heartbeat);
            }

            // Don't close connection; more data to come
            return new Status(0);
        } else {
            if (options.isIncludeConflicts()) {
                connection.setResponseBody(new Body(responseBodyForChangesWithConflicts(changes, since)));
            } else {
                connection.setResponseBody(new Body(responseBodyForChanges(changes, since)));
            }
            return new Status(Status.OK);
        }
    }

    private void startHeartbeat(long interval) {
        if (interval <= 0)
            return;

        stopHeartbeat();
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                synchronized (databaseChangesLongpollLock) {
                    OutputStream os = connection.getResponseOutputStream();
                    if (os != null) {
                        try {
                            Log.v(TAG, "[%s] Sent heart beat!", this);
                            os.write("\r\n".getBytes());
                            os.flush();
                        } catch (IOException e) {
                            Log.w(TAG, "IOException writing to internal streams: " + e.getMessage());
                        } finally {
                            // no close outputstream, OutputStream might be re-used
                        }
                    }
                }
            }
        }, interval, interval);
    }

    private void stopHeartbeat() {
        if (timer != null) {
            timer.cancel();
            timer.purge();
            timer = null;
        }
    }

    /**
     * DOCUMENT REQUESTS: *
     */

    private String getRevIDFromIfMatchHeader() {
        String ifMatch = getRequestHeaderValue("If-Match");
        if (ifMatch == null) {
            return null;
        }
        // Value of If-Match is an ETag, so have to trim the quotes around it:
        if (ifMatch.length() > 2 && ifMatch.startsWith("\"") && ifMatch.endsWith("\"")) {
            return ifMatch.substring(1, ifMatch.length() - 2);
        } else {
            return null;
        }
    }

    public Status do_GET_Document(Database _db, String docID, String _attachmentName) {
        try {
            // http://wiki.apache.org/couchdb/HTTP_Document_API#GET
            boolean isLocalDoc = docID.startsWith("_local");
            EnumSet<TDContentOptions> options = getContentOptions();
            String openRevsParam = getQuery("open_revs");
            boolean mustSendJSON = explicitlyAcceptsType(CONTENT_TYPE_JSON);
            if (openRevsParam == null || isLocalDoc) {
                // Regular GET:
                String revID = getQuery("rev");  // often null
                RevisionInternal rev;
                boolean includeAttachments = false;
                boolean sendMultipart = false; // TODO: Router does not support multi-part now
                if (isLocalDoc) {
                    // query only -> not required synchronized
                    rev = db.getLocalDocument(docID, revID);
                } else {
                    includeAttachments = options.contains(TDContentOptions.TDIncludeAttachments);
                    if (includeAttachments) {
                        //sendMultipart = !mustSendJSON;
                        options.remove(TDContentOptions.TDIncludeAttachments);
                    }
                    // query only -> not required synchronized
                    rev = db.getDocument(docID, revID, true);
                    if (rev != null) {
                        rev = applyOptionsToRevision(options, rev);
                    }
                }

                if (rev == null)
                    return new Status(Status.NOT_FOUND);
                if (cacheWithEtag(rev.getRevID()))
                    return new Status(Status.NOT_MODIFIED);  // set ETag and check conditional GET

                if (!isLocalDoc && includeAttachments) {
                    int minRevPos = 1;
                    List<String> attsSince = parseJSONRevArrayQuery(getQuery("atts_since"));
                    String ancestorID = db.getStore().findCommonAncestorOf(rev, attsSince);
                    if (ancestorID != null)
                        minRevPos = Revision.generationFromRevID(ancestorID) + 1;
                    RevisionInternal expandedRev = rev.copy();
                    Status status = new Status(Status.OK);
                    if (!db.expandAttachments(expandedRev, minRevPos, sendMultipart,
                            !getBooleanQuery("att_encoding_info"), status))
                        return status;
                    rev = expandedRev;
                }

                // TODO: Needs to support multi-part

                connection.setResponseBody(rev.getBody());
            } else {
                List<Map<String, Object>> result = null;
                if ("all".equals(openRevsParam)) {
                    // Get all conflicting revisions:
                    RevisionList allRevs = db.getStore().getAllRevisions(docID, true);
                    result = new ArrayList<Map<String, Object>>(allRevs.size());
                    for (RevisionInternal rev : allRevs) {

                        try {
                            // loadRevisionBody is synchronized with store instance.
                            db.loadRevisionBody(rev);
                        } catch (CouchbaseLiteException e) {
                            if (e.getCBLStatus().getCode() != Status.INTERNAL_SERVER_ERROR) {
                                Map<String, Object> dict = new HashMap<String, Object>();
                                dict.put("missing", rev.getRevID());
                                result.add(dict);
                            } else {
                                throw e;
                            }
                        }

                        Map<String, Object> dict = new HashMap<String, Object>();
                        dict.put("ok", rev.getProperties());
                        result.add(dict);
                    }
                } else {
                    // ?open_revs=[...] returns an array of revisions of the document:
                    List<String> openRevs = (List<String>) getJSONQuery("open_revs");
                    if (openRevs == null) {
                        return new Status(Status.BAD_REQUEST);
                    }
                    result = new ArrayList<Map<String, Object>>(openRevs.size());
                    for (String revID : openRevs) {
                        RevisionInternal rev = db.getDocument(docID, revID, true);
                        if (rev != null) {
                            Map<String, Object> dict = new HashMap<String, Object>();
                            dict.put("ok", rev.getProperties());
                            result.add(dict);
                        } else {
                            Map<String, Object> dict = new HashMap<String, Object>();
                            dict.put("missing", revID);
                            result.add(dict);
                        }
                    }
                }
                String acceptMultipart = getMultipartRequestType();
                if (acceptMultipart != null) {
                    //FIXME figure out support for multipart
                    throw new UnsupportedOperationException();
                } else {
                    connection.setResponseBody(new Body(result));
                }
            }
            return new Status(Status.OK);
        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }
    }

    /**
     * in CBL_Router+Handlers.m
     * - (CBL_Revision*) applyOptions: (CBLContentOptions)options
     * toRevision: (CBL_Revision*)rev
     * status: (CBLStatus*)outStatus
     * 1.1 or earlier => Database.extraPropertiesForRevision()
     */
    private RevisionInternal applyOptionsToRevision(EnumSet<TDContentOptions> options, RevisionInternal rev) {
        if (options != null && (
                options.contains(TDContentOptions.TDIncludeLocalSeq) ||
                        options.contains(TDContentOptions.TDIncludeRevs) ||
                        options.contains(TDContentOptions.TDIncludeRevsInfo) ||
                        options.contains(TDContentOptions.TDIncludeConflicts) ||
                        options.contains(TDContentOptions.TDBigAttachmentsFollow))) {

            Map<String, Object> dst = new HashMap<String, Object>();
            dst.putAll(rev.getProperties());
            Store store = db.getStore();

            if (options.contains(TDContentOptions.TDIncludeLocalSeq)) {
                dst.put("_local_seq", rev.getSequence());
                rev.setProperties(dst);
            }
            if (options.contains(TDContentOptions.TDIncludeRevs)) {
                List<RevisionInternal> revs = db.getRevisionHistory(rev);
                Map<String, Object> historyDict = RevisionUtils.makeRevisionHistoryDict(revs);
                dst.put("_revisions", historyDict);
                rev.setProperties(dst);
            }
            if (options.contains(TDContentOptions.TDIncludeRevsInfo)) {
                List<Object> revsInfo = new ArrayList<Object>();
                List<RevisionInternal> revs = db.getRevisionHistory(rev);
                for (RevisionInternal historicalRev : revs) {
                    Map<String, Object> revHistoryItem = new HashMap<String, Object>();
                    String status = "available";
                    if (historicalRev.isDeleted()) {
                        status = "deleted";
                    }
                    if (historicalRev.isMissing()) {
                        status = "missing";
                    }
                    revHistoryItem.put("rev", historicalRev.getRevID());
                    revHistoryItem.put("status", status);
                    revsInfo.add(revHistoryItem);
                }
                dst.put("_revs_info", revsInfo);
                rev.setProperties(dst);
            }
            if (options.contains(TDContentOptions.TDIncludeConflicts)) {
                RevisionList revs = store.getAllRevisions(rev.getDocID(), true);
                if (revs.size() > 1) {
                    List<String> conflicts = new ArrayList<String>();
                    for (RevisionInternal aRev : revs) {
                        if (aRev.equals(rev) || aRev.isDeleted()) {
                            // don't add in this case
                        } else {
                            conflicts.add(aRev.getRevID());
                        }
                    }
                    dst.put("_conflicts", conflicts);
                }
                rev.setProperties(dst);
            }
            if (options.contains(TDContentOptions.TDBigAttachmentsFollow)) {
                RevisionInternal nuRev = new RevisionInternal(dst);
                Status outStatus = new Status(Status.OK);
                if (!db.expandAttachments(nuRev, 0, false, getBooleanQuery("att_encoding_info"), outStatus))
                    return null;
                rev = nuRev;
            }
        }
        return rev;
    }

    public Status do_GET_Attachment(Database _db, String docID, String _attachmentName) {
        try {
            // http://wiki.apache.org/couchdb/HTTP_Document_API#GET
            EnumSet<TDContentOptions> options = getContentOptions();
            options.add(TDContentOptions.TDNoBody);
            String revID = getQuery("rev");  // often null
            RevisionInternal rev = db.getDocument(docID, revID, false);
            if (rev == null) {
                return new Status(Status.NOT_FOUND);
            }
            if (cacheWithEtag(rev.getRevID())) {
                return new Status(Status.NOT_MODIFIED);  // set ETag and check conditional GET
            }

            String acceptEncoding = connection.getRequestProperty("accept-encoding");
            // getAttachment is safe. this could be static method??
            AttachmentInternal attachment = db.getAttachment(rev, _attachmentName);
            if (attachment == null) {
                return new Status(Status.NOT_FOUND);
            }

            String type = attachment.getContentType();
            if (type != null) {
                connection.getResHeader().add("Content-Type", type);
            }
            if (acceptEncoding != null && acceptEncoding.contains("gzip") &&
                    attachment.getEncoding() == AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP) {
                connection.getResHeader().add("Content-Encoding", "gzip");
            }

            dontOverwriteBody = true;
            connection.setResponseInputStream(attachment.getContentInputStream());
            return new Status(Status.OK);

        } catch (CouchbaseLiteException e) {
            return e.getCBLStatus();
        }
    }

    /**
     * NOTE this departs from the iOS version, returning revision, passing status back by reference
     * <p/>
     * - (CBLStatus) update: (CBLDatabase*)db
     * docID: (NSString*)docID
     * body: (CBL_Body*)body
     * deleting: (BOOL)deleting
     * allowConflict: (BOOL)allowConflict
     * createdRev: (CBL_Revision**)outRev
     * error: (NSError**)outError
     */
    private RevisionInternal update(Database _db, String docID, Body body, boolean deleting,
                                    boolean allowConflict, Status outStatus) {

        if (body != null && !body.isValidJSON()) {
            outStatus.setCode(Status.BAD_JSON);
            return null;
        }

        boolean isLocalDoc = docID != null && docID.startsWith(("_local"));
        String prevRevID;

        if (!deleting) {
            Boolean deletingBoolean = (Boolean) body.getPropertyForKey("_deleted");
            deleting = (deletingBoolean != null && deletingBoolean.booleanValue());
            if (docID == null) {
                if (isLocalDoc) {
                    outStatus.setCode(Status.METHOD_NOT_ALLOWED);
                    return null;
                }
                // POST's doc ID may come from the _id field of the JSON body, else generate a random one.
                docID = (String) body.getPropertyForKey("_id");
                if (docID == null) {
                    if (deleting) {
                        outStatus.setCode(Status.BAD_REQUEST);
                        return null;
                    }
                    docID = Misc.CreateUUID();
                }
            }
            // PUT's revision ID comes from the JSON body.
            prevRevID = (String) body.getPropertyForKey("_rev");
        } else {
            // DELETE's revision ID comes from the ?rev= query param
            prevRevID = getQuery("rev");
        }

        // A backup source of revision ID is an If-Match header:
        if (prevRevID == null) {
            prevRevID = getRevIDFromIfMatchHeader();
        }

        RevisionInternal rev = new RevisionInternal(docID, null, deleting);
        rev.setBody(body);

        RevisionInternal result = null;
        try {
            if (isLocalDoc) {
                // NOTE: putLocalRevision() does not use transaction internally with obeyMVCC=true

                final Database fDb = _db;
                final RevisionInternal _rev = rev;
                final String _prevRevID = prevRevID;
                final List<RevisionInternal> _revs = new ArrayList<RevisionInternal>();
                try {
                    fDb.getStore().runInTransaction(new TransactionalTask() {
                        @Override
                        public boolean run() {
                            try {
                                RevisionInternal r = fDb.getStore().putLocalRevision(_rev, _prevRevID, true);
                                _revs.add(r);
                                return true;
                            } catch (CouchbaseLiteException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    // success
                    if (_revs.size() > 0)
                        result = _revs.get(0);
                } catch (RuntimeException ex) {
                    if (ex.getCause() != null &&
                            ex.getCause().getCause() != null &&
                            ex.getCause().getCause() instanceof CouchbaseLiteException)
                        throw (CouchbaseLiteException) ex.getCause().getCause();
                    else
                        throw new CouchbaseLiteException(ex, Status.INTERNAL_SERVER_ERROR);
                }
            } else {
                // putRevision() uses transaction internally.
                result = _db.putRevision(rev, prevRevID, allowConflict);
            }
            if (deleting) {
                outStatus.setCode(Status.OK);
            } else {
                outStatus.setCode(Status.CREATED);
            }
        } catch (CouchbaseLiteException e) {
            if (e.getCBLStatus() != null && e.getCBLStatus().getCode() == Status.CONFLICT) {
                // conflict is not critical error for replicators, not print stack trace
                Log.w(TAG, "Error updating doc: %s", docID);
            } else {
                Log.e(TAG, "Error updating doc: %s", e, docID);
            }
            outStatus.setCode(e.getCBLStatus().getCode());
        }

        return result;
    }

    /**
     * in CBL_Router+Handlers.m
     * - (CBLStatus) update: (CBLDatabase*)db
     * docID: (NSString*)docID
     * body: (CBL_Body*)body
     * deleting: (BOOL)deleting
     * error: (NSError**)outError
     */
    private Status update(Database _db, String docID, Body body, boolean deleting) {
        Status status = new Status();

        if (docID != null && docID.isEmpty() == false) {
            // On PUT/DELETE, get revision ID from either ?rev= query or doc body:
            String revParam = getQuery("rev");
            String ifMatch = getRequestHeaderValue("If-Match");
            if (ifMatch != null) {
                if (revParam == null)
                    revParam = ifMatch;
                else if (!ifMatch.equals(revParam))
                    return new Status(Status.BAD_REQUEST);
            }
            if (revParam != null && body != null) {
                String revProp = (String) body.getProperties().get("_rev");
                if (revProp == null) {
                    // No _rev property in body, so use ?rev= query param instead:
                    body.getProperties().put("_rev", revParam);
                    //body = new Body(bodyDict);
                } else if (!revParam.equals(revProp)) {
                    throw new IllegalArgumentException("Mismatch between _rev and rev");
                }
            }
        }

        RevisionInternal rev = update(_db, docID, body, deleting, false, status);
        if (status.isSuccessful()) {
            cacheWithEtag(rev.getRevID());  // set ETag
            if (!deleting) {
                URL url = connection.getURL();
                String urlString = url.toExternalForm();
                if (docID != null) {
                    urlString += '/' + rev.getDocID();
                    try {
                        url = new URL(urlString);
                    } catch (MalformedURLException e) {
                        Log.w("Malformed URL", e);
                    }
                }
                setResponseLocation(url);
            }
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("ok", true);
            result.put("id", rev.getDocID());
            result.put("rev", rev.getRevID());
            connection.setResponseBody(new Body(result));
        }
        return status;
    }

    public Status do_PUT_Document(Database _db, String docID, String _attachmentName)
            throws CouchbaseLiteException {
        Status status = new Status(Status.CREATED);
        Map<String, Object> body = getBodyAsDictionary();
        if (body == null) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        if (getQuery("new_edits") == null ||
                (getQuery("new_edits") != null && (Boolean.valueOf(getQuery("new_edits"))))) {
            // Regular PUT
            status = update(_db, docID, new Body(body), false);
        } else {
            // PUT with new_edits=false -- forcible insertion of existing revision:
            Body revBody = new Body(body);
            RevisionInternal rev = new RevisionInternal(revBody);
            if (rev.getRevID() == null || rev.getDocID() == null || !rev.getDocID().equals(docID)) {
                throw new CouchbaseLiteException(Status.BAD_REQUEST);
            }
            List<String> history = Database.parseCouchDBRevisionHistory(revBody.getProperties());
            // forceInsert uses transaction internally, not necessary to apply synchronized
            db.forceInsert(rev, history, source);
        }
        return status;
    }

    public Status do_DELETE_Document(Database _db, String docID, String _attachmentName) {
        return update(_db, docID, null, true);
    }

    /**
     * @param contentStream if null, delete attachment. if not null, update attachment
     */
    private Status updateAttachment(String attachment,
                                    String docID,
                                    InputStream contentStream)
            throws CouchbaseLiteException {
        Status status = new Status(Status.OK);
        String revID = getQuery("rev");
        if (revID == null)
            revID = getRevIDFromIfMatchHeader();

        if (revID == null || revID.length() == 0)
            throw new CouchbaseLiteException(Status.BAD_REQUEST);

        BlobStoreWriter body = null;
        if (contentStream != null) {
            body = new BlobStoreWriter(db.getAttachmentStore());
            ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
            try {
                StreamUtils.copyStream(contentStream, dataStream);
                body.appendData(dataStream.toByteArray());
                body.finish();
            } catch (Exception e) {
                throw new CouchbaseLiteException(e.getCause(), Status.BAD_ATTACHMENT);
            } finally {
                try {
                    dataStream.close();
                } catch (IOException e) {
                    throw new CouchbaseLiteException(e.getCause(), Status.BAD_ATTACHMENT);
                }
            }
        }

        // updateAttachment uses transaction internally, not necessary to be synchronized
        RevisionInternal rev = db.updateAttachment(
                attachment,
                body,
                getRequestHeaderContentType(),
                AttachmentInternal.AttachmentEncoding.AttachmentEncodingNone,
                docID,
                revID,
                null);
        Map<String, Object> resultDict = new HashMap<String, Object>();
        resultDict.put("ok", true);
        resultDict.put("id", rev.getDocID());
        resultDict.put("rev", rev.getRevID());
        connection.setResponseBody(new Body(resultDict));
        cacheWithEtag(rev.getRevID());
        if (contentStream != null) {
            setResponseLocation(connection.getURL());
        }
        return status;
    }

    public Status do_PUT_Attachment(Database _db, String docID, String _attachmentName)
            throws CouchbaseLiteException {
        return updateAttachment(_attachmentName, docID, connection.getRequestInputStream());
    }

    public Status do_DELETE_Attachment(Database _db, String docID, String _attachmentName)
            throws CouchbaseLiteException {
        return updateAttachment(_attachmentName, docID, null);
    }

    /**
     * VIEW QUERIES: *
     */
    private View compileView(String viewName, Map<String, Object> viewProps) {
        String language = (String) viewProps.get("language");
        if (language == null) {
            language = "javascript";
        }
        String mapSource = (String) viewProps.get("map");
        if (mapSource == null) {
            return null;
        }
        Mapper mapBlock = View.getCompiler().compileMap(mapSource, language);
        if (mapBlock == null) {
            Log.w(TAG, "View %s has unknown map function: %s", viewName, mapSource);
            return null;
        }
        String reduceSource = (String) viewProps.get("reduce");
        Reducer reduceBlock = null;
        if (reduceSource != null) {
            reduceBlock = View.getCompiler().compileReduce(reduceSource, language);
            if (reduceBlock == null) {
                Log.w(TAG, "View %s has unknown reduce function: %s", viewName, reduceBlock);
                return null;
            }
        }

        View view = db.getView(viewName);
        view.setMapReduce(mapBlock, reduceBlock, "1");
        String collation = (String) viewProps.get("collation");
        if ("raw".equals(collation)) {
            view.setCollation(View.TDViewCollation.TDViewCollationRaw);
        }
        return view;
    }

    private Status queryDesignDoc(String designDoc, String viewName, List<Object> keys)
            throws CouchbaseLiteException {
        String tdViewName = String.format(Locale.ENGLISH, "%s/%s", designDoc, viewName);
        // getExistingView is not thread-safe, but not access to db. In the database, it should protect instance variable
        View view = db.getExistingView(tdViewName);
        if (view == null || view.getMap() == null) {
            // No TouchDB view is defined, or it hasn't had a map block assigned;
            // see if there's a CouchDB view definition we can compile:
            RevisionInternal rev = db.getDocument(String.format(Locale.ENGLISH, "_design/%s", designDoc), null, true);
            if (rev == null) {
                return new Status(Status.NOT_FOUND);
            }
            Map<String, Object> views = (Map<String, Object>) rev.getProperties().get("views");
            Map<String, Object> viewProps = (Map<String, Object>) views.get(viewName);
            if (viewProps == null) {
                return new Status(Status.NOT_FOUND);
            }
            // If there is a CouchDB view, see if it can be compiled from source:
            view = compileView(tdViewName, viewProps);
            if (view == null) {
                return new Status(Status.INTERNAL_SERVER_ERROR);
            }
        }

        QueryOptions options = new QueryOptions();

        //if the view contains a reduce block, it should default to reduce=true
        if (view.getReduce() != null) {
            options.setReduce(true);
        }

        if (!getQueryOptions(options)) {
            return new Status(Status.BAD_REQUEST);
        }
        if (keys != null) {
            options.setKeys(keys);
        }

        // updateIndex() uses transaction internally, not necessary to apply syncrhonized.
        view.updateIndex();

        long lastSequenceIndexed = view.getLastSequenceIndexed();

        // Check for conditional GET and set response Etag header:
        if (keys == null) {
            long eTag = options.isIncludeDocs() ? db.getLastSequenceNumber() : lastSequenceIndexed;
            if (cacheWithEtag(String.format(Locale.ENGLISH, "%d", eTag))) {
                return new Status(Status.NOT_MODIFIED);
            }
        }

        // convert from QueryRow -> Map
        List<QueryRow> queryRows = view.query(options);
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (QueryRow queryRow : queryRows) {
            rows.add(queryRow.asJSONDictionary());
        }

        Map<String, Object> responseBody = new HashMap<String, Object>();
        responseBody.put("rows", rows);
        responseBody.put("total_rows", view.getCurrentTotalRows());
        responseBody.put("offset", options.getSkip());
        if (options.isUpdateSeq()) {
            responseBody.put("update_seq", lastSequenceIndexed);
        }
        connection.setResponseBody(new Body(responseBody));
        return new Status(Status.OK);
    }

    public Status do_GET_DesignDocument(Database _db, String designDocID, String viewName)
            throws CouchbaseLiteException {
        return queryDesignDoc(designDocID, viewName, null);
    }

    public Status do_POST_DesignDocument(Database _db, String designDocID, String viewName)
            throws CouchbaseLiteException {
        Map<String, Object> body = getBodyAsDictionary();
        if (body == null) {
            return new Status(Status.BAD_REQUEST);
        }
        List<Object> keys = (List<Object>) body.get("keys");
        return queryDesignDoc(designDocID, viewName, keys);
    }

    public void setSource(URL source) {
        this.source = source;
    }

    @Override
    public String toString() {
        String url = "Unknown";
        if (connection != null && connection.getURL() != null) {
            url = connection.getURL().toExternalForm();
        }
        return String.format(Locale.ENGLISH, "Router [%s]", url);
    }
}
