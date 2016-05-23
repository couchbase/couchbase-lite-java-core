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

import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.util.Log;

/**
 * @exclude
 */
@InterfaceAudience.Private
public class ChangeTrackerBackoff {

    private static int MAX_SLEEP_MILLISECONDS = 5 * 60 * 1000;  // 5 mins
    private int numAttempts = 0;

    public void resetBackoff() {
        numAttempts = 0;
    }

    public int getSleepMilliseconds() {

        int result = (int) (Math.pow(numAttempts, 2) - 1) / 2;

        result *= 100;

        if (result < MAX_SLEEP_MILLISECONDS) {
            increaseBackoff();
        }

        result = Math.abs(result);

        return result;
    }

    public void sleepAppropriateAmountOfTime() {
        try {
            int sleepMilliseconds = getSleepMilliseconds();
            if (sleepMilliseconds > 0) {
                Log.d(Log.TAG_CHANGE_TRACKER, "%s: sleeping for %d", this, sleepMilliseconds);
                Thread.sleep(sleepMilliseconds);
            }
        } catch (InterruptedException e1) {
        }
    }

    private void increaseBackoff() {
        numAttempts += 1;
    }

    public int getNumAttempts() {
        return numAttempts;
    }
}
