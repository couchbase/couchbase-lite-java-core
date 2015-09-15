//
//  DatabaseUpgrade.java
//
//  Created by Hideki Itakura on 7/21/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite;

import com.couchbase.lite.internal.AttachmentInternal;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLiteStorageEngine;
import com.couchbase.lite.storage.SQLiteStorageEngineFactory;
import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.util.Log;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Imports from the v1.0 SQLite database format into a CBLDatabase.
 * This class is optional: the source file does not need to be built into the app or the
 * Couchbase Lite library. If it's not present, Couchbase Lite will ignore old v1.0 databases
 * instead of importing them.
 */
final public class DatabaseUpgrade {
    private static String TAG = Log.TAG_DATABASE;

    private Manager manager;
    private Database db;
    private String path;
    private int numDocs = 0;
    private int numRevs = 0;
    private boolean canRemoveOldAttachmentsDir = true;
    private SQLiteStorageEngine storageEngine = null;

    protected DatabaseUpgrade(Manager manager, Database db, String sqliteFile) {
        this.manager = manager;
        this.db = db;
        this.path = sqliteFile;
        this.numDocs = 0;
        this.numRevs = 0;
        this.canRemoveOldAttachmentsDir = true;
        this.storageEngine = null;
    }

    protected int getNumDocs() {
        return numDocs;
    }

    protected int getNumRevs() {
        return numRevs;
    }

    protected boolean canRemoveOldAttachmentsDir() {
        return canRemoveOldAttachmentsDir;
    }

    protected boolean importData() {
        try {
            // Create the storage engine for source database
            SQLiteStorageEngineFactory factory = manager.getContext().getSQLiteStorageEngineFactory();
            try {
                storageEngine = factory.createStorageEngine(manager.isEnableStorageEncryption());
            } catch (CouchbaseLiteException e) {
                Log.e(TAG, "Upgrade failed: Unable to create a storage engine", e);
                return false;
            }

            if (storageEngine == null) {
                Log.e(TAG, "Upgrade failed: Unable to create a storage engine");
                return false;
            }

            // Open source (SQLite) database:
            // TODO: We also need encryption key for database upgrade.
            if (!storageEngine.open(path, null)) {
                Log.e(TAG, "Upgrade failed: Couldn't open new db: %s", path);
                return false;
            }

            // Open destination database:
            boolean isOpen = false;
            Throwable error = null;
            try {
                isOpen = db.open();
            } catch (CouchbaseLiteException e) {
                error = e;
            }
            if (!isOpen) {
                Log.e(TAG, "Upgrade failed:  Couldn't open new db: %s", db.toString(), error);
                return false;
            }

            // Move attachment storage directory:
            if (!moveAttachmentsDir())
                return false;

            // Upgrade documents:
            // CREATE TABLE docs (doc_id INTEGER PRIMARY KEY, docid TEXT UNIQUE NOT NULL);
            String sql = "SELECT doc_id, docid FROM docs";
            String[] args = {};
            final Cursor cursor = storageEngine.rawQuery(sql, args);
            try {
                boolean ret = db.runInTransaction(new TransactionalTask() {
                    @Override
                    public boolean run() {
                        cursor.moveToNext();
                        while (!cursor.isAfterLast()) {
                            long docNumericID = cursor.getLong(0);
                            String docID = cursor.getString(1);
                            Status status = importDoc(docID, docNumericID);
                            if (status.isError())
                                return false;
                            cursor.moveToNext();
                        }
                        return true;
                    }
                });
                if (!ret)
                    return false;
            } finally {
                if (cursor != null)
                    cursor.close();
            }

            // Upgrade local docs:
            importLocalDocs();

            // Upgrade info (public/private UUIDs):
            importInfo();

            return true;
        } finally {
            if (storageEngine != null && storageEngine.isOpen())
                storageEngine.close();
        }
    }

    protected void backOut() {
        // Move attachments dir back to old path:
        File newAttachmentsPath = new File(db.getAttachmentStorePath());
        if (newAttachmentsPath.canRead()) {
            File oldAttachmentsPath = new File(FileDirUtils.getPathWithoutExt(path) + " attachments");
            if (canRemoveOldAttachmentsDir)
                newAttachmentsPath.renameTo(oldAttachmentsPath);
        }
        try {
            db.delete();
        } catch (CouchbaseLiteException e) {
            Log.w(TAG, "Failed to delete Database: %s: %s", db, e);
        }
    }

    private boolean moveAttachmentsDir() {
        File oldAttachmentsPath = new File(FileDirUtils.getPathWithoutExt(path) + " attachments");
        if (!oldAttachmentsPath.canRead())
            return true;
        File newAttachmentsPath = new File(db.getAttachmentStorePath());
        Log.v(TAG, "Upgrade: Moving '%s' to '%s'",
                oldAttachmentsPath, newAttachmentsPath);
        FileDirUtils.deleteRecursive(newAttachmentsPath);

        try {
            if (canRemoveOldAttachmentsDir) {
                FileDirUtils.copyFolder(oldAttachmentsPath, newAttachmentsPath);
                FileDirUtils.deleteRecursive(oldAttachmentsPath);
            } else {
                FileDirUtils.copyFolder(oldAttachmentsPath, newAttachmentsPath);
            }
        } catch (IOException e) {
            Log.w(TAG, "Upgrade failed: Couldn't move attachments: %s", e);
            return false;
        }

        return true;
    }

    private Status importDoc(String docID, long docNumericID) {
        // Check if the attachments table exists or not:

        boolean attachmentsTableExists = this.attachmentsTableExists();

        // CREATE TABLE revs (
        //  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        //  doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE,
        //  revid TEXT NOT NULL COLLATE REVID,
        //  parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL,
        //  current BOOLEAN,
        //  deleted BOOLEAN DEFAULT 0,
        //  json BLOB,
        //  no_attachments BOOLEAN,
        //  UNIQUE (doc_id, revid) );
        class Pair {
            Pair(String revID, Long parentSeq) {
                this.revID = revID;
                this.parentSeq = parentSeq;
            }

            String revID;
            Long parentSeq;
        }
        Map<Long, Pair> tree = new HashMap<Long, Pair>();
        String sql = "SELECT sequence, revid, parent, current, deleted, json, no_attachments" +
                " FROM revs WHERE doc_id=? ORDER BY sequence";
        String[] args = {String.valueOf(docNumericID)};
        Cursor cursor = storageEngine.rawQuery(sql, args);
        try {
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                long sequence = cursor.getLong(0);
                String revID = cursor.getString(1);
                long parentSeq = cursor.getLong(2);
                boolean current = cursor.getInt(3) != 0;
                boolean noAtts = cursor.getInt(6) != 0;
                if (current) {
                    // Add a leaf revision:
                    boolean deleted = cursor.getInt(4) != 0;
                    byte[] json = cursor.getBlob(5);
                    if (json == null)
                        json = "{}".getBytes();
                    if (attachmentsTableExists) {
                        json = this.addAttachmentsToSequence(sequence, json);
                        if (json == null)
                            return new Status(Status.BAD_JSON);
                    } else {//.NET v1.1 database already has attachments bundled in the JSON data:
                        if (!noAtts) {
                            json = this.updateAttachmentFollowsInJson(json);
                            if (json == null)
                                return new Status(Status.BAD_JSON);
                        }
                    }

                    RevisionInternal rev = new RevisionInternal(docID, revID, deleted);
                    rev.setJSON(json);

                    List<String> history = new ArrayList<String>();
                    history.add(revID);
                    while (parentSeq > 0) {
                        Pair ancestor = tree.get(parentSeq);
                        history.add(ancestor.revID);
                        parentSeq = ancestor.parentSeq;
                    }
                    try {
                        db.forceInsert(rev, history, null);
                    } catch (CouchbaseLiteException ex) {
                        return ex.getCBLStatus();
                    }
                    ++numRevs;
                } else {
                    tree.put(sequence, new Pair(revID, parentSeq));
                }
                cursor.moveToNext();
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }
        ++numDocs;
        return new Status(Status.OK);
    }

    private byte[] updateAttachmentFollowsInJson(byte[] json) {
        Map<String, Object> object = null;
        try {
            object = Manager.getObjectMapper().readValue(json, Map.class);
        } catch (IOException e) {
            return null;
        }
        Map<String, Object> attachments = (Map<String, Object>) object.get("_attachments");
        for (String key : attachments.keySet()) {
            Map<String, Object> attachment = (Map<String, Object>) attachments.get(key);
            attachment.put("follows", true);
            attachment.remove("stub");
        }
        try {
            return Manager.getObjectMapper().writeValueAsBytes(object);
        } catch (IOException e) {
            return null;
        }
    }

    private byte[] addAttachmentsToSequence(long sequence, byte[] json) {
        // CREATE TABLE attachments (
        //  sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE,
        //  filename TEXT NOT NULL,
        //  key BLOB NOT NULL,
        //  type TEXT,
        //  length INTEGER NOT NULL,
        //  revpos INTEGER DEFAULT 0,
        //  encoding INTEGER DEFAULT 0,
        //  encoded_length INTEGER );

        Map<String, Object> attachments = new HashMap<String, Object>();

        String sql = "SELECT filename, key, type, length," +
                " revpos, encoding, encoded_length FROM attachments WHERE sequence=?";
        String[] args = {String.valueOf(sequence)};
        Cursor cursor = storageEngine.rawQuery(sql, args);
        try {
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                String name = cursor.getString(0);
                byte[] key = cursor.getBlob(1);
                String mimeType = cursor.getString(2);
                long length = cursor.getLong(3);
                int revpos = cursor.getInt(4);
                int encoding = cursor.getInt(5);
                long encodingLength = cursor.getLong(6);

                BlobKey blobKey = new BlobKey(key);
                String digest = blobKey.base64Digest();

                Map<String, Object> att = new HashMap<String, Object>();
                att.put("type", mimeType);
                att.put("digest", digest);
                att.put("length", length);
                att.put("revpos", revpos);
                att.put("follows", true);
                att.put("encoding", AttachmentInternal.AttachmentEncoding.values()[encoding] ==
                        AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP ?
                        "gzip" : null);
                att.put("encoded_length", AttachmentInternal.AttachmentEncoding.values()[encoding] ==
                        AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP ?
                        encodingLength : null);

                attachments.put(name, att);
                cursor.moveToNext();
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }


        if (attachments.size() > 0) {
            // Splice attachment JSON into the document JSON:
            Map<String, Object> attachmentsWrapper = new HashMap<String, Object>();
            attachmentsWrapper.put("_attachments", attachments);
            byte[] extraJSON;
            try {
                extraJSON = Manager.getObjectMapper().writeValueAsBytes(attachmentsWrapper);
            } catch (IOException e) {
                return null;
            }

            int jsonLength = json.length;
            int extraLength = extraJSON.length;
            if (jsonLength == 2) { // Original JSON was empty
                return extraJSON;
            }
            byte[] newJson = new byte[jsonLength + extraLength - 1];
            System.arraycopy(json, 0, newJson, 0, jsonLength - 1);  // Copy json w/o trailing '}'
            newJson[jsonLength - 1] = ',';  // Add a ','
            System.arraycopy(extraJSON, 1, newJson, jsonLength, extraLength - 1);
            return newJson;
        } else {
            return json;
        }
    }

    private boolean attachmentsTableExists() {
        String sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachments'";
        Cursor cursor = storageEngine.rawQuery(sql, null);
        try {
            if (cursor.moveToNext())
                return true;
            else
                return false;
        } finally {
            if (cursor != null)
                cursor.close();
        }
    }

    private void importLocalDocs() {
        // CREATE TABLE localdocs (
        //  docid TEXT UNIQUE NOT NULL,
        //  revid TEXT NOT NULL COLLATE REVID,
        //  json BLOB );

        String sql = "SELECT docid, json FROM localdocs";
        Cursor cursor = storageEngine.rawQuery(sql, null);
        try {
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                String docID = cursor.getString(0);
                byte[] json = cursor.getBlob(1);
                Log.v(TAG, "Upgrading local doc '%s'", docID);
                try {
                    Map<String, Object> props = Manager.getObjectMapper().readValue(json, Map.class);
                    if (!db.putLocalDocument(docID, props)) {
                        Log.w(TAG, "Couldn't import local doc '%s'", docID);
                    }
                } catch (Exception e) {
                    Log.w(TAG, "Couldn't import local doc '%s': '%s'", docID, e);
                }
                cursor.moveToNext();
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }
    }

    private void importInfo() {
        // CREATE TABLE info (key TEXT PRIMARY KEY, value TEXT);
        String sql = "SELECT key, value FROM info";
        Cursor cursor = storageEngine.rawQuery(sql, null);
        try {
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                db.getStore().setInfo(cursor.getString(0), cursor.getString(1));
                cursor.moveToNext();
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }
    }
}
