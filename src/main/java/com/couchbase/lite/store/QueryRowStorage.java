//
//  QueryRowStorage.java
//
//  Created by Hideki Itakura on 6/10/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite.store;

import com.couchbase.lite.CouchbaseLiteException;

import java.util.Map;

/**
 * Storage for a CBLQueryRow. Instantiated by a CBL_ViewStorage when it creates a CBLQueryRow.
 */
public interface QueryRowStorage {
    /**
     * Given the raw data of a row's value, returns YES if this is a non-JSON placeholder representing
     * the entire document. If so, the CBLQueryRow will not parse this data but will instead fetch the
     * document's body from the database and use that as its value.
     */
    boolean rowValueIsEntireDoc(byte[] valueData);

    /**
     * Parses a "normal" (not entire-doc) row value into a JSON-compatible object.
     */
    Object parseRowValue(byte[] valueData);

    /**
     * Fetches a document's body; called when the row value represents the entire document.
     * @param docID The document ID
     * @param sequence The sequence representing this revision
     * @return The document properties, or nil on error
     * @throws CouchbaseLiteException
     */
    Map<String, Object> documentProperties(String docID, long sequence) throws CouchbaseLiteException;
}
