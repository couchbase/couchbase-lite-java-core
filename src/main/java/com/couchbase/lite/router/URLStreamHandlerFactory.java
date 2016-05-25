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
package com.couchbase.lite.router;

import java.net.URL;
import java.net.URLStreamHandler;


public class URLStreamHandlerFactory implements java.net.URLStreamHandlerFactory {

    public static final String SCHEME = "cblite";  // eg, cblite://

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        if(SCHEME.equals(protocol)) {
            return new URLHandler();
        }
        return null;
    }

    public static void registerSelfIgnoreError() {
        try {
            URL.setURLStreamHandlerFactory(new URLStreamHandlerFactory());
        } catch (Error e) {
            //usually you should never catch an Error
            //but I can't see how to avoid this
        }
    }
}
