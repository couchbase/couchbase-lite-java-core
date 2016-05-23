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
