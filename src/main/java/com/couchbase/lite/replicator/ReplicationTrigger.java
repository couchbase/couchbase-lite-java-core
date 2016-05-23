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
package com.couchbase.lite.replicator;

/**
 * The various triggers that a Replication state machine responds to
 */
enum ReplicationTrigger {
    START,
    WAITING_FOR_CHANGES,
    RESUME, // send the RESUME trigger when a replication is IDLE but has new items to process
    GO_OFFLINE,
    GO_ONLINE,
    STOP_GRACEFUL,
    STOP_IMMEDIATE
}

