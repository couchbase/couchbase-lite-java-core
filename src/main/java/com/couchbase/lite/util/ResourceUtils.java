/**
 * Created by Andrew Reslan.
 *
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
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

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

public class ResourceUtils {

    /**
     * List directory contents for a resource folder. Not recursive.
     *
     * @author Andrew Reslan
     * @param clazz Any java class that lives in the same place as the resources folder
     * @param path Should end with "/", but not start with one.
     * @return An array of the name of each member item, or null if path does not denote a directory
     * @throws URISyntaxException
     * @throws IOException
     */
    public static String[] getResourceListing(Class clazz, String path) throws URISyntaxException, IOException {

        URL dirURL = clazz.getClassLoader().getResource(path);
        if (dirURL != null && dirURL.getProtocol().equals("file")) {
            return new File(dirURL.toURI()).list();
        }


        throw new UnsupportedOperationException("Cannot list files for URL "+dirURL);
    }
}
