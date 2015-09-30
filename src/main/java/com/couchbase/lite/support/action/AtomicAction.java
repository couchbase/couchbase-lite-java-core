/**
 * Created by Pasin Suriyentrakorn on 8/29/15.
 * <p/>
 * Copyright (c) 2015 Couchbase, Inc All rights reserved.
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

package com.couchbase.lite.support.action;

/**
 * An abstraction whose instances can perform some action and back it out.
 */
public interface AtomicAction {
    /**
     * Performs the action. Behavior should be all-or-nothing: if the action doesn't succeed, it
     * should restore any temporary state to what it was before, before throwing an exception.
     * @throws Exception
     */
    void perform() throws ActionException;

    /**
     * Backs out the completed action. This will be called if a subsequent action has failed.
     * @throws Exception
     */
    void backout() throws ActionException;

    /**
     * Cleans up after all actions have completed. This may involve releasing/deleting
     * any temporary resources being kept around to fulfil a backOut request.
     * @throws Exception
     */
    void cleanup() throws ActionException;
}
