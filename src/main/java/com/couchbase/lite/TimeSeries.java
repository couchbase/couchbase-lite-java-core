/**
 * Copyright (c) 2016 Couchbase, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.couchbase.lite;

import com.couchbase.lite.replicator.Replication;
import com.couchbase.lite.util.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Efficiently stores small time-stamped JSON values into a database,
 * and can replicate them to a server (purging them as soon as they're pushed.)
 *
 * https://github.com/couchbaselabs/couchbase-lite-api/wiki/Log-and-Time-Series-Syncing
 */
public class TimeSeries {

    ////////////////////////////////////////////////////////////
    // Constants
    ////////////////////////////////////////////////////////////
    public static final String TAG = Log.TAG_TIMESERIES;

    ////////////////////////////////////////////////////////////
    // Member variables
    ////////////////////////////////////////////////////////////

    /**
     * The "type" property that will be added to documents.
     */
    private String docType;

    /**
     * A prefix for the IDs of documents created by this object.
     * Defaults to "TS-" + docType + "-".
     */
    private String docIDPrefix;

    /**
     * The latest error encountered.
     * Observable. (Note: May be modified on any thread.)
     */
    private Exception lastError;


    private Database db;
    FileOutputStream out;
    int eventsInFile;
    List docsToAdd;

    ////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////

    /**
     * Initializes a new TimeSeries.
     *
     * @param db      The database to store events in.
     * @param docType The document "type" property to use. Must be non-nil, and must not be used by
     *                any other documents or time-series in the database.
     * @throws FileNotFoundException will be thrown if file is not openable.
     */
    public TimeSeries(Database db, String docType) throws FileNotFoundException {
        if (db == null) throw new IllegalArgumentException("db");
        if (docType == null) throw new IllegalArgumentException("docType");

        String filename = String.format(Locale.ENGLISH, "TS-%s.tslog", docType);
        File path = new File(db.getPath(), filename);
        out = new FileOutputStream(path, true);

        this.db = db;
        this.docType = docType;
        this.docIDPrefix = String.format(Locale.ENGLISH, "TS-%s-", docType);
    }

    ////////////////////////////////////////////////////////////
    // Public Methods
    ////////////////////////////////////////////////////////////

    /**
     * Adds an event, timestamped with the current time. Can be called on any thread.
     */
    public void addEvent(Map<String, Object> event) {

    }

    /**
     * Adds an event with a custom timestamp (which must be greater than the last timestamp.)
     * Can be called on any thread.
     */
    public void addEvent(Map<String, Object> event, Date time) {

    }

    interface FlushAsyncCallback {
        void onFlashued(boolean status);
    }

    /**
     * Writes all pending events to documents asynchronously, then calls the onFlushed block
     * (with parameter YES on success, NO if there were any errors.)
     * Can be called on any thread.
     */
    public void flushAsync(FlushAsyncCallback callback) {

    }

    /**
     * Writes all pending events to documents before returning.
     * Must be called on the database's thread.
     *
     * @return true on success, false if there were any errors.
     */
    public boolean flush() {
        return false;
    }

    /**
     * Stops the CBLTimeSeries, immediately flushing all pending events.
     */
    public void stop() {

    }

    //// REPLICATION:

    /**
     * Creates, but does not start, a new CBLReplication to push the events to a remote database.
     * You can customize the replication's properties before starting it, but don't alter the
     * filter or remove the existing customProperties.
     *
     * @param remoteURL       The URL of the remote database to push to.
     * @param purgeWhenPushed If YES, time-series documents will be purged from the local database
     *                        immediately after they've been pushed. Use this if you don't need them anymore.
     * @return The Replication instance.
     */
    public Replication createPushReplication(URL remoteURL, boolean purgeWhenPushed) {
        return null;
    }

    //// QUERYING:

    /**
     * Enumerates the events stored in the database from time t0 to t1, inclusive.
     * Each event returned from the NSEnumerator is an NSDictionary, as provided to -addEvent,
     * with a key "t" whose value is the absolute time as an NSDate.
     *
     * @param startDate The starting time (or nil to start from the beginning.)
     * @param endDate   The ending time (or nil to continue till the end.)
     * @return An enumerator of NSDictionaries, one per event.
     */
    public List eventsFromDate(Date startDate, Date endDate) {
        return null;
    }

    ////////////////////////////////////////////////////////////
    // Setter / Getter methods
    ////////////////////////////////////////////////////////////

    public String getDocType() {
        return docType;
    }

    public String getDocIDPrefix() {
        return docIDPrefix;
    }

    public Exception getLastError() {
        return lastError;
    }
}
