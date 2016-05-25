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
 * A delegate that can be invoked to compile source code into a ReplicationFilter.
 */
public interface ReplicationFilterCompiler {
    /**
     *
     * Compile Filter Function
     *
     * @param source The source code to compile into a ReplicationFilter.
     * @param language The language of the source.
     * @return A compiled ReplicationFilter.
     */
    ReplicationFilter compileFilterFunction(String source, String language);
}
