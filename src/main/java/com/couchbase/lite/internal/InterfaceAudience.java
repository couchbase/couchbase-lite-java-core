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
package com.couchbase.lite.internal;


/**
 * Annotations to help mark methods as being public or private.  This is needed to
 * help with the issue that Java's scoping is not very complete. One is often forced to
 * make a class public in order for other internal components to use it. It does not have
 * friends or sub-package-private like C++
 *
 * Motivated by http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/InterfaceClassification.html
 */
public class InterfaceAudience {

    /**
     * Intended for use by any project or application.
     */
    public @interface Public {};

    /**
     * Intended for use only within Couchbase Lite itself.
     */
    public @interface Private {};
}
