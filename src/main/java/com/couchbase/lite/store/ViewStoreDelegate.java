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

import com.couchbase.lite.Mapper;
import com.couchbase.lite.Reducer;

/**
 * Delegate of a CBL_ViewStorage instance. CBLView implements this.
 */
public interface ViewStoreDelegate {
    /**
     * The current map block. Never nil.
     */
    Mapper getMap();

    /**
     * The current reduce block, or nil if there is none.
     */
    Reducer getReduce();

    /**
     * The current map version string. If this changes, the storage's -setVersion: method will be
     * called to notify it, so it can invalidate the _index.
     */
    String getMapVersion();

    /**
     * The document "type" property values this view is filtered to (nil if none.)
     */
    String getDocumentType();
}
