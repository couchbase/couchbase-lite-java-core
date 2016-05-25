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

/**
 * A delegate that can validate a key/value change.
 */
public interface ChangeValidator {

    /**
     * Validate a change
     *
     * @param key The key of the value being changed.
     * @param oldValue The old value.
     * @param newValue The new value.
     * @return true if the change is valid, false otherwise
     */
    boolean validateChange(String key, Object oldValue, Object newValue);
}
