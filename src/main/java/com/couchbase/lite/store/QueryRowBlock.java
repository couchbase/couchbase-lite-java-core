//
//  QueryRowBlock.java
//
//  Created by Hideki Itakura on 6/23/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite.store;

import com.couchbase.lite.Status;
import com.couchbase.lite.storage.Cursor;

interface QueryRowBlock {
    Status onRow(byte[] keyData, byte[] valueData, String docID, Cursor cursor);
}
