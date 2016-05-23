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
package com.couchbase.lite.util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by hideki on 7/23/15.
 */
public class JSONUtils {

    private static long kObjectOverhead = 20;

    public static long estimate(Object obj) {
        if (obj == null) return 0;

        // String
        if (obj instanceof String) {
            return kObjectOverhead + 2 * ((String) obj).length();
        }
        // Number is abstract class of all number types such as Long, Float, ...
        else if (obj instanceof Number) {
            return kObjectOverhead + 8;
        }
        // Map (Dictionary)
        else if (obj instanceof Map) {
            long size = kObjectOverhead;
            Map map = (Map) obj;
            Iterator itr = map.keySet().iterator();
            while (itr.hasNext()) {
                Object key = itr.next();
                size += estimate(key) + estimate(map.get(key));
            }
            return size;
        }
        // List (Array)
        else if (obj instanceof List) {
            long size = kObjectOverhead;
            for (Object item : (List) obj) {
                size += estimate(item);
            }
            return size;
        } else {
            return 0;
        }
    }
}
