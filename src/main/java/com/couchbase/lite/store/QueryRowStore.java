//
//  QueryRowStore.java
//
//  Created by Hideki Itakura on 6/23/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite.store;

import java.util.Map;

public interface QueryRowStore {
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
     *
     * @param docID    The document ID
     * @param sequence The sequence representing this revision
     * @return The document properties, or nil on error
     */
    Map<String, Object> getDocumentProperties(String docID, long sequence);
}
