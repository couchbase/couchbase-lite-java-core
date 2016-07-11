/**
 * Copyright (c) 2016 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.couchbase.lite;

import com.couchbase.lite.auth.Authorizer;
import com.couchbase.lite.auth.FacebookAuthorizer;
import com.couchbase.lite.auth.PersonaAuthorizer;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.replicator.Replication;
import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.Version;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.StreamUtils;
import com.couchbase.lite.util.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Top-level CouchbaseLite object; manages a collection of databases as a CouchDB server does.
 */
public final class Manager {

    public static final String PRODUCT_NAME = "CouchbaseLite";

    protected static final String kV1DBExtension = ".cblite";  // Couchbase Lite 1.0
    protected static final String kDBExtension   = ".cblite2"; // Couchbase Lite 1.2 or later (for iOS 1.1 or later)

    public static final ManagerOptions DEFAULT_OPTIONS = new ManagerOptions();
    public static final String LEGAL_CHARACTERS = "[^a-z]{1,}[^a-z0-9_$()/+-]*$";
    public static String USER_AGENT = null;

    public static final String SQLITE_STORAGE = "SQLite";
    public static final String FORESTDB_STORAGE = "ForestDB";

    // NOTE: Jackson is thread-safe http://wiki.fasterxml.com/JacksonFAQThreadSafety
    private static final ObjectMapper mapper = new ObjectMapper();

    private ManagerOptions options;
    private File directoryFile;
    private Map<String, Database> databases;
    private Map<String, Object> encryptionKeys;
    private List<Replication> replications;
    private ScheduledExecutorService workExecutor;
    private HttpClientFactory defaultHttpClientFactory;
    private Context context;
    private String storageType;

    ///////////////////////////////////////////////////////////////////////////
    // APIs
    // https://github.com/couchbaselabs/couchbase-lite-api/blob/master/gen/md/Database.md
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Constructor
     *
     * @throws UnsupportedOperationException - not currently supported
     * @exclude
     */
    @InterfaceAudience.Public
    public Manager() {
        final String detailMessage = "Parameterless constructor is not a valid API call on Android. " +
                " Pure java version coming soon.";
        throw new UnsupportedOperationException(detailMessage);
    }

    /**
     * Constructor
     *
     * @throws java.lang.SecurityException - Runtime exception that can be thrown by File.mkdirs()
     */
    @InterfaceAudience.Public
    public Manager(Context context, ManagerOptions options) throws IOException {

        Log.d(Database.TAG, "Starting Manager version: %s", Manager.VERSION);

        this.context = context;
        this.directoryFile = context.getFilesDir();
        this.options = (options != null) ? options : DEFAULT_OPTIONS;
        this.databases = new HashMap<String, Database>();
        this.encryptionKeys = new HashMap<String, Object>();
        this.replications = new ArrayList<Replication>();

        if (!directoryFile.exists()) {
            directoryFile.mkdirs();
        }
        if (!directoryFile.isDirectory()) {
            throw new IOException(String.format(Locale.ENGLISH, "Unable to create directory for: %s", directoryFile));
        }

        upgradeOldDatabaseFiles(directoryFile);

        // this must be a single threaded executor due to contract w/ Replication object
        // which must run on either:
        // - a shared single threaded executor
        // - its own single threaded executor
        workExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "CBLManagerWorkExecutor");
            }
        });
    }

    ///////////////////////////////////////////////////////////////////////////
    // Constants
    ///////////////////////////////////////////////////////////////////////////

    public static final String VERSION = Version.VERSION;

    ///////////////////////////////////////////////////////////////////////////
    // Class Members - Properties
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get shared instance
     *
     * @throws UnsupportedOperationException - not currently supported
     * @exclude
     */
    @InterfaceAudience.Public
    public static Manager getSharedInstance() {
        final String detailMessage = "getSharedInstance() is not a valid API call on Android. " +
                " Pure java version coming soon";
        throw new UnsupportedOperationException(detailMessage);
    }

    /**
     * Get the default storage type.
     * @return Storage type.
     */
    @InterfaceAudience.Public
    public String getStorageType() {
        return storageType;
    }

    /**
     * Set default storage engine type for newly-created databases.
     * There are two options, "SQLite" (the default) or "ForestDB".
     * @param storageType
     */
    @InterfaceAudience.Public
    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Class Members - Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Enable logging for a particular tag / loglevel combo
     *
     * @param tag      Used to identify the source of a log message.  It usually identifies
     *                 the class or activity where the log call occurs.
     * @param logLevel The loglevel to enable.  Anything matching this loglevel
     *                 or having a more urgent loglevel will be emitted.  Eg, Log.VERBOSE.
     */
    public static void enableLogging(String tag, int logLevel) {
        Log.enableLogging(tag, logLevel);
    }

    /**
     * Returns YES if the given name is a valid database name.
     * (Only the characters in "abcdefghijklmnopqrstuvwxyz0123456789_$()+-/" are allowed.)
     */
    @InterfaceAudience.Public
    public static boolean isValidDatabaseName(String databaseName) {
        if (databaseName.length() > 0 && databaseName.length() < 240 &&
                containsOnlyLegalCharacters(databaseName) &&
                Character.isLowerCase(databaseName.charAt(0))) {
            return true;
        }
        return databaseName.equals(Replication.REPLICATOR_DATABASE_NAME);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Members - Properties
    ///////////////////////////////////////////////////////////////////////////

    /**
     * An array of the names of all existing databases.
     */
    @InterfaceAudience.Public
    public List<String> getAllDatabaseNames() {
        String[] databaseFiles = directoryFile.list(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String filename) {
                if (filename.endsWith(Manager.kDBExtension)) {
                    return true;
                }
                return false;
            }
        });
        List<String> result = new ArrayList<String>();
        for (String databaseFile : databaseFiles) {
            String trimmed = databaseFile.substring(0, databaseFile.length() - Manager.kDBExtension.length());
            String replaced = trimmed.replace(':', '/');
            result.add(replaced);
        }
        Collections.sort(result);
        return Collections.unmodifiableList(result);
    }

    /**
     * The root directory of this manager (as specified at initialization time.)
     */
    @InterfaceAudience.Public
    public File getDirectory() {
        return directoryFile;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Members - Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Releases all resources used by the Manager instance and closes all its databases.
     */
    @InterfaceAudience.Public
    public void close() {
        Log.d(Database.TAG, "Closing " + this);
        // Close all database:
        // Snapshot of the current open database to avoid concurrent modification as
        // the database will be forgotten (removed from the databases map) when it is closed:
        Database[] openDbs = databases.values().toArray(new Database[databases.size()]);
        for (Database database : openDbs) {
            database.close();
        }
        databases.clear();

        // Stop reachability:
        context.getNetworkReachabilityManager().stopListening();

        // Shutdown ScheduledExecutorService:
        if (workExecutor != null && !workExecutor.isShutdown()) {
            Utils.shutdownAndAwaitTermination(workExecutor);
        }
        Log.d(Database.TAG, "Closed " + this);
    }

    /**
     * <p>
     *     Returns the database with the given name, or creates it if it doesn't exist.
     *     Multiple calls with the same name will return the same {@link Database} instance.
     * <p/>
     * <p>
     *     This is equivalent to calling {@link #openDatabase(String, DatabaseOptions)}
     *     with a default set of options with the `Create` flag set.
     * </p>
     * <p>
     *     NOTE: Database names may not contain capital letters.
     * </p>
     */
    @InterfaceAudience.Public
    public Database getDatabase(String name) throws CouchbaseLiteException {
        DatabaseOptions options = getDefaultOptions(name);
        options.setCreate(true);
        return openDatabase(name, options);
    }

    /**
     * <p>
     *     Returns the database with the given name, or null if it doesn't exist.
     *     Multiple calls with the same name will return the same {@link Database} instance.
     * <p/>
     * <p>
     *     This is equivalent to calling {@link #openDatabase(String, DatabaseOptions)}
     *     with a default set of options.
     * </p>
     */
    @InterfaceAudience.Public
    public Database getExistingDatabase(String name) throws CouchbaseLiteException {
        DatabaseOptions options = getDefaultOptions(name);
        return openDatabase(name, options);
    }

    /**
     * Returns the database with the given name. If the database is not yet open, the options given
     * will be applied; if it's already open, the options are ignored.
     * Multiple calls with the same name will return the same {@link Database} instance.
     * @param name The name of the database. May NOT contain capital letters!
     * @param options Options to use when opening, such as the encryption key; if null, a default
     *                set of options will be used.
     * @return The database instance.
     * @throws CouchbaseLiteException thrown when there is an error.
     */
    @InterfaceAudience.Public
    public Database openDatabase(String name, DatabaseOptions options)
            throws CouchbaseLiteException {
        if (options == null)
            options = getDefaultOptions(name);
        Database db = getDatabase(name, !options.isCreate());
        if (db != null && !db.isOpen()) {
            db.open(options);
            registerEncryptionKey(options.getEncryptionKey(), name);
        }
        return db;
    }

    /**
     * This method has been superseded by {@link #openDatabase(String, DatabaseOptions)}.
     *
     * Registers an encryption key for a database. This must be called _before_ opening an encrypted
     * database, or before creating a database that's to be encrypted.
     * If the key is incorrect (or no key is given for an encrypted database), the subsequent call
     * to open the database will fail with an error with code 401.
     * To use this API, the database storage engine must support encryption, and the
     * ManagerOptions.EnableStorageEncryption property must be set to true. Otherwise opening
     * the database will fail with an error.
     * @param keyOrPassword The encryption key in the form of an String (a password) or an
     *                      byte[] object exactly 32 bytes in length (a raw AES key.)
     *                      If a string is given, it will be internally converted to a raw key
     *                      using 64,000 rounds of PBKDF2 hashing.
     *                      A null value is legal, and clears a previously-registered key.
     * @param databaseName  The name of the database.
     * @return              True if the key can be used, False if it's not in a legal form
     *                      (e.g. key as a byte[] is not 32 bytes in length.)
     */
    @InterfaceAudience.Public
    public boolean registerEncryptionKey(Object keyOrPassword, String databaseName) {
        if (databaseName == null)
            return false;
        if (keyOrPassword != null) {
            encryptionKeys.put(databaseName, keyOrPassword);
        } else
            encryptionKeys.remove(databaseName);
        return true;
    }

    /**
     * Replaces or installs a database from a file.
     * <p/>
     * This is primarily used to install a canned database
     * on first launch of an app, in which case you should first check .exists to avoid replacing the
     * database if it exists already. The canned database would have been copied into your app bundle
     * at build time. This property is deprecated for the new .cblite2 database file. If the database
     * file is a directory and has the .cblite2 extension,
     * use -replaceDatabaseNamed:withDatabaseDir:error: instead.
     *
     * @param databaseName      The name of the target Database to replace or create.
     * @param databaseStream    InputStream on the source Database file.
     * @param attachmentStreams Map of the associated source Attachments, or null if there are no attachments.
     *                          The Map key is the name of the attachment, the map value is an InputStream for
     *                          the attachment contents. If you wish to control the order that the attachments
     *                          will be processed, use a LinkedHashMap, SortedMap or similar and the iteration order
     *                          will be honoured.
     */
    @InterfaceAudience.Public
    public void replaceDatabase(String databaseName,
                                InputStream databaseStream,
                                Map<String, InputStream> attachmentStreams)
            throws CouchbaseLiteException {
        replaceDatabase(databaseName, databaseStream,
                attachmentStreams == null ? null : attachmentStreams.entrySet().iterator());
    }

    /**
     * Replaces or installs a database from a file.
     *
     * This is primarily used to install a canned database
     * on first launch of an app, in which case you should first check .exists to avoid replacing the
     * database if it exists already. The canned database would have been copied into your app bundle
     * at build time. If the database file is not a directory and has the .cblite extension,
     * use -replaceDatabaseNamed:withDatabaseFile:withAttachments:error: instead.
     *
     * @param databaseName The name of the database to replace.
     * @param databaseDir Path of the database directory that should replace it.
     * @return YES if the database was copied, NO if an error occurred.
     */
    @InterfaceAudience.Public
    public boolean replaceDatabase(String databaseName, String databaseDir) {
        Database db = getDatabase(databaseName, false);
        if(db == null)
            return false;

        File dir = new File(databaseDir);
        if(!dir.exists()){
            Log.w(Database.TAG, "Database file doesn't exist at path : %s", databaseDir);
            return false;
        }
        if (!dir.isDirectory()) {
            Log.w(Database.TAG, "Database file is not a directory. " +
                    "Use -replaceDatabaseNamed:withDatabaseFilewithAttachments:error: instead.");
            return false;
        }

        File destDir = new File(db.getPath());
        File srcDir = new File(databaseDir);
        if(destDir.exists()) {
            if (!FileDirUtils.deleteRecursive(destDir)) {
                Log.w(Database.TAG, "Failed to delete file/directly: " + destDir);
                return false;
            }
        }
        try {
            FileDirUtils.copyFolder(srcDir, destDir);
        } catch (IOException e) {
            Log.w(Database.TAG, "Failed to copy directly from " + srcDir + " to " + destDir, e);
            return false;
        }

        try {
            db.open();
        } catch (CouchbaseLiteException e) {
            Log.w(Database.TAG, "Failed to open database", e);
            return false;
        }

        /* TODO: Currently Java implementation is different from iOS, needs to catch up.
        if(!db.saveLocalUUIDInLocalCheckpointDocument()){
            Log.w(Database.TAG, "Failed to replace UUIDs");
            return false;
        }
        */

        if(!db.replaceUUIDs()){
            Log.w(Database.TAG, "Failed to replace UUIDs");
            db.close();
            return false;
        }

        // close so app can (re)open db with its preferred options:
        db.close();
        return true;
    }

    ///////////////////////////////////////////////////////////////////////////
    // End of APIs
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Public but Not API
    ///////////////////////////////////////////////////////////////////////////

    @InterfaceAudience.Private
    DatabaseOptions getDefaultOptions(String databaseName) {
        DatabaseOptions options = new DatabaseOptions();
        options.setEncryptionKey(encryptionKeys.get(databaseName));
        return options;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static ObjectMapper getObjectMapper() {
        return mapper;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public HttpClientFactory getDefaultHttpClientFactory() {
        return defaultHttpClientFactory;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public void setDefaultHttpClientFactory(HttpClientFactory defaultHttpClientFactory) {
        this.defaultHttpClientFactory = defaultHttpClientFactory;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Collection<Database> allOpenDatabases() {
        return databases.values();
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Map<String, Object> getEncryptionKeys() {
        return Collections.unmodifiableMap(encryptionKeys) ;
    }

    /**
     * Asynchronously dispatches a callback to run on a background thread. The callback will be passed
     * Database instance.  There is not currently a known reason to use it, it may not make
     * sense on the Android API, but it was added for the purpose of having a consistent API with iOS.
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public Future runAsync(String databaseName, final AsyncTask function) throws CouchbaseLiteException {
        final Database database = getDatabase(databaseName);
        return runAsync(new Runnable() {
            @Override
            public void run() {
                function.run(database);
            }
        });
    }

    /**
     * Instantiates a database but doesn't open the file yet.
     * in CBLManager.m
     * - (CBLDatabase*) _databaseNamed: (NSString*)name
     * mustExist: (BOOL)mustExist
     * error: (NSError**)outError
     *
     * @exclude
     */
    @InterfaceAudience.Private
    public synchronized Database getDatabase(String name, boolean mustExist) {
        if (options.isReadOnly())
            mustExist = true;
        Database db = databases.get(name);
        if (db == null) {
            if (!isValidDatabaseName(name))
                throw new IllegalArgumentException("Invalid database name: " + name);
            String path = pathForDatabaseNamed(name);
            if (path == null)
                return null;
            db = new Database(path, name, this, options.isReadOnly());
            if (mustExist && !db.exists()) {
                Log.i(Database.TAG, "mustExist is true and db (%s) does not exist", name);
                return null;
            }
            db.setName(name);
            databases.put(name, db);
        }
        return db;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Replication getReplicator(Map<String, Object> properties) throws CouchbaseLiteException {

        // TODO: in the iOS equivalent of this code, there is: {@"doc_ids", _documentIDs}) - write unit test that detects this bug
        // TODO: ditto for "headers"

        Authorizer authorizer = null;
        Replication repl = null;
        URL remote = null;

        Map<String, Object> remoteMap;

        Map<String, Object> sourceMap = parseSourceOrTarget(properties, "source");
        Map<String, Object> targetMap = parseSourceOrTarget(properties, "target");

        String source = (String) sourceMap.get("url");
        String target = (String) targetMap.get("url");

        Boolean createTargetBoolean = (Boolean) properties.get("create_target");
        boolean createTarget = (createTargetBoolean != null && createTargetBoolean.booleanValue());

        Boolean continuousBoolean = (Boolean) properties.get("continuous");
        boolean continuous = (continuousBoolean != null && continuousBoolean.booleanValue());

        Boolean cancelBoolean = (Boolean) properties.get("cancel");
        boolean cancel = (cancelBoolean != null && cancelBoolean.booleanValue());

        // Map the 'source' and 'target' JSON params to a local database and remote URL:
        if (source == null || target == null) {
            throw new CouchbaseLiteException("source and target are both null", new Status(Status.BAD_REQUEST));
        }

        boolean push = false;
        Database db = null;
        String remoteStr = null;

        if (Manager.isValidDatabaseName(source)) {
            db = getExistingDatabase(source);
            remoteStr = target;
            push = true;
            remoteMap = targetMap;
        } else {
            remoteStr = source;
            if (createTarget && !cancel) {
                boolean mustExist = false;
                db = getDatabase(target, mustExist);
                db.open();
            } else {
                db = getExistingDatabase(target);
            }
            if (db == null) {
                throw new CouchbaseLiteException("database is null", new Status(Status.NOT_FOUND));
            }
            remoteMap = sourceMap;
        }

        // Can't specify both a filter and doc IDs
        if (properties.get("filter") != null && properties.get("doc_ids") != null)
            throw new CouchbaseLiteException("Can't specify both a filter and doc IDs",
                    new Status(Status.BAD_REQUEST));

        try {
            remote = new URL(remoteStr);
        } catch (MalformedURLException e) {
            throw new CouchbaseLiteException("malformed remote url: " + remoteStr,
                    new Status(Status.BAD_REQUEST));
        }
        if (remote == null) {
            throw new CouchbaseLiteException("remote URL is null: " + remoteStr,
                    new Status(Status.BAD_REQUEST));
        }

        Map<String, Object> authMap = (Map<String, Object>) remoteMap.get("auth");
        if (authMap != null) {

            Map<String, Object> persona = (Map<String, Object>) authMap.get("persona");
            if (persona != null) {
                String email = (String) persona.get("email");
                authorizer = new PersonaAuthorizer(email);
            }
            Map<String, Object> facebook = (Map<String, Object>) authMap.get("facebook");
            if (facebook != null) {
                String email = (String) facebook.get("email");
                authorizer = new FacebookAuthorizer(email);
            }
            authorizer.setRemoteURL(remote);
            authorizer.setLocalUUID(db.publicUUID());
        }



        if (!cancel) {
            repl = db.getReplicator(remote, getDefaultHttpClientFactory(), push, continuous);
            if (repl == null) {
                throw new CouchbaseLiteException("unable to create replicator with remote: " + remote,
                        new Status(Status.INTERNAL_SERVER_ERROR));
            }

            if (authorizer != null) {
                repl.setAuthenticator(authorizer);
            }

            Map<String, Object> headers = null;
            if (remoteMap != null) {
                headers = (Map) remoteMap.get("headers");
            }

            if (headers != null && !headers.isEmpty()) {
                repl.setHeaders(headers);
            }

            String filterName = (String) properties.get("filter");
            if (filterName != null) {
                repl.setFilter(filterName);
                Map<String, Object> filterParams = (Map<String, Object>) properties.get("query_params");
                if (filterParams != null) {
                    repl.setFilterParams(filterParams);
                }
            }

            // docIDs
            if(properties.get("doc_ids") != null) {
                if(properties.get("doc_ids") instanceof List){
                    List<String> docIds = (List<String>)properties.get("doc_ids");
                    repl.setDocIds(docIds);
                }
            }

            String remoteUUID = (String) properties.get("remoteUUID");
            if (remoteUUID != null) {
                repl.setRemoteUUID(remoteUUID);
            }

            if (push) {
                repl.setCreateTarget(createTarget);
            }
        } else {
            // Cancel replication:
            repl = db.getActiveReplicator(remote, push);
            if (repl == null) {
                throw new CouchbaseLiteException("unable to lookup replicator with remote: " + remote,
                        new Status(Status.NOT_FOUND));
            }
        }

        return repl;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public ScheduledExecutorService getWorkExecutor() {
        return workExecutor;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public Context getContext() {
        return context;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public int getExecutorThreadPoolSize() {
        return this.options.getExecutorThreadPoolSize();
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal (protected or private) Methods
    ///////////////////////////////////////////////////////////////////////////

    @InterfaceAudience.Private
    private void replaceDatabase(String databaseName,
                                 InputStream databaseStream,
                                 Iterator<Map.Entry<String, InputStream>> attachmentStreams)
            throws CouchbaseLiteException {
        try {
            Database db = getDatabase(databaseName, false);

            String dstDbPath = FileDirUtils.getPathWithoutExt(db.getPath()) + kV1DBExtension;
            String dstAttsPath = FileDirUtils.getPathWithoutExt(dstDbPath) + " attachments";

            OutputStream destStream = new FileOutputStream(new File(dstDbPath));
            StreamUtils.copyStream(databaseStream, destStream);
            File attachmentsFile = new File(dstAttsPath);
            FileDirUtils.deleteRecursive(attachmentsFile);
            if (!attachmentsFile.exists()) {
                attachmentsFile.mkdirs();
            }
            if (attachmentStreams != null) {
                StreamUtils.copyStreamsToFolder(attachmentStreams, attachmentsFile);
            }
            if (!upgradeV1Database(databaseName, dstDbPath)) {
                throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
            }
            db.open();
            db.replaceUUIDs();
        } catch (FileNotFoundException e) {
            Log.e(Database.TAG, "Error replacing the database: %s", e, databaseName);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } catch (IOException e) {
            Log.e(Database.TAG, "Error replacing the database: %s", e, databaseName);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private static boolean containsOnlyLegalCharacters(String databaseName) {
        Pattern p = Pattern.compile("^[abcdefghijklmnopqrstuvwxyz0123456789_$()+-/]+$");
        Matcher matcher = p.matcher(databaseName);
        return matcher.matches();
    }

    /**
     * Scan my dir for SQLite-based databases from Couchbase Lite 1.0 and upgrade them:
     * <p/>
     * in CBLManager.m
     * - (void) upgradeOldDatabaseFiles
     *
     * @exclude
     */
    @InterfaceAudience.Private
    private void upgradeOldDatabaseFiles(File directory) {
        File[] files = directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                return name.endsWith(kV1DBExtension);
            }
        });

        for (File file : files) {
            String filename = file.getName();
            String name = nameOfDatabaseAtPath(filename);
            String oldDbPath = new File(directory, filename).getAbsolutePath();
            upgradeDatabase(name, oldDbPath, true);
        }
    }

    /**
     * in CBLManager.m
     * - (BOOL) upgradeDatabaseNamed: (NSString*)name
     * atPath: (NSString*)dbPath
     * error: (NSError**)outError
     */
    private boolean upgradeDatabase(String name, String dbPath, boolean close) {
        Log.v(Log.TAG_DATABASE, "CouchbaseLite: Upgrading database at %s ...", dbPath);
        if (!name.equals("_replicator")) {
            // Create and open new CBLDatabase:
            Database db = getDatabase(name, false);
            if (db == null) {
                Log.w(Log.TAG_DATABASE, "Upgrade failed: Creating new db failed");
                return false;
            }
            if (!db.exists()) {
                // Upgrade the old database into the new one:
                DatabaseUpgrade upgrader = new DatabaseUpgrade(this, db, dbPath);
                if (!upgrader.importData()) {
                    upgrader.backOut();
                    return false;
                }
            }
            if (close)
                db.close();
        }

        // Remove old database file and its SQLite side files:
        moveSQLiteDbFiles(dbPath, null);

        if (dbPath.endsWith(kV1DBExtension)) {
            String oldAttachmentsName = FileDirUtils.getDatabaseNameFromPath(dbPath) + " attachments";
            File oldAttachmentsDir = new File(directoryFile, oldAttachmentsName);
            if (oldAttachmentsDir.exists())
                FileDirUtils.deleteRecursive(oldAttachmentsDir);
        }

        Log.v(Log.TAG_DATABASE, "    ...success!");
        return true;
    }

    private boolean upgradeV1Database(String name, String dbPath) {
        if (dbPath.endsWith(kV1DBExtension)) {
            return upgradeDatabase(name, dbPath, false);
        } else {
            // Gracefully skipping the upgrade:
            Log.w(Log.TAG_DATABASE, "Upgrade skipped: Database file extension is not %s", kDBExtension);
            return true;
        }
    }

    private static void moveSQLiteDbFiles(String oldDbPath, String newDbPath) {
        for (String suffix : Arrays.asList("", "-wal", "-shm", "-journal")) {
            File oldFile = new File(oldDbPath + suffix);
            if (!oldFile.exists())
                continue;
            if (newDbPath != null)
                oldFile.renameTo(new File(newDbPath + suffix));
            else
                oldFile.delete();
        }
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected Future runAsync(Runnable runnable) {
        synchronized (workExecutor) {
            if (!workExecutor.isShutdown()) {
                return workExecutor.submit(runnable);
            } else {
                return null;
            }
        }
    }

    /**
     * in CBLManager.m
     * - (NSString*) pathForDatabaseNamed: (NSString*)name
     *
     * @exclude
     */
    @InterfaceAudience.Private
    private static String nameOfDatabaseAtPath(String path) {
        String name = FileDirUtils.getDatabaseNameFromPath(path);
        return isWindows() ? name.replace('/', '.') : name.replace('/', ':');
    }

    /**
     * in CBLManager.m
     * - (NSString*) pathForDatabaseNamed: (NSString*)name
     *
     * @exclude
     */
    @InterfaceAudience.Private
    private String pathForDatabaseNamed(String name) {
        if ((name == null) || (name.length() == 0) || Pattern.matches(LEGAL_CHARACTERS, name))
            return null;
        // NOTE: CouchDB allows forward slash as part of database name.
        //       However, ':' is illegal character on Windows platform.
        //       For Windows, substitute with period '.'
        name = isWindows() ? name.replace('/', '.') : name.replace('/', ':');
        String result = directoryFile.getPath() + File.separator + name + Manager.kDBExtension;
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    private static Map<String, Object> parseSourceOrTarget(Map<String, Object> properties, String key) {
        Map<String, Object> result = new HashMap<String, Object>();

        Object value = properties.get(key);

        if (value instanceof String) {
            result.put("url", value);
        } else if (value instanceof Map) {
            result = (Map<String, Object>) value;
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected void forgetDatabase(Database db) {
        // remove from cached list of dbs
        databases.remove(db.getName());

        // remove from list of replications
        // TODO: should there be something that actually stops the replication(s) first?
        Iterator<Replication> replicationIterator = this.replications.iterator();
        while (replicationIterator.hasNext()) {
            Replication replication = replicationIterator.next();
            if (replication.getLocalDatabase().getName().equals(db.getName())) {
                replicationIterator.remove();
            }
        }

        // Remove registered encryption key if available:
        encryptionKeys.remove(db.getName());
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    protected boolean isAutoMigrateBlobStoreFilename() {
        return this.options.isAutoMigrateBlobStoreFilename();
    }

    private static String OS = System.getProperty("os.name").toLowerCase();

    /**
     * Check if platform is Windows
     */
    @InterfaceAudience.Private
    private static boolean isWindows() {
        return (OS.indexOf("win") >= 0);
    }


    /**
     * Return User-Agent value
     * Format: ex: CouchbaseLite/1.2 (Java Linux/MIPS 1.2.1/3382EFA)
     */
    public static String getUserAgent() {
        if (USER_AGENT == null) {
            USER_AGENT = String.format(Locale.ENGLISH, "%s/%s (%s/%s)",
                    PRODUCT_NAME,
                    Version.SYNC_PROTOCOL_VERSION,
                    Version.getVersionName(),
                    Version.getCommitHash());
        }
        return USER_AGENT;
    }
}
