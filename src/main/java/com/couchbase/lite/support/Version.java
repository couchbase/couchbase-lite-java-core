package com.couchbase.lite.support;

import com.couchbase.lite.util.Log;

public class Version {

    private static final String VERSION_NAME="${VERSION_NAME}";  // replaced during build process
    private static final String VERSION_CODE="${VERSION_CODE}";  // replaced during build process

    public static String getVersionName() {
        if (VERSION_NAME == "${VERSION_NAME}") {
            return "devbuild";
        }
        return VERSION_NAME;
    }

    public static int getVersionCode() {
        if (VERSION_CODE == "${VERSION_CODE}") {
            return 0;
        }
        try {
            Integer.parseInt(VERSION_CODE);
        } catch (NumberFormatException e) {
            Log.e(Log.TAG, "Cannot parse version code: %s", VERSION_CODE);
        }
        return -1;
    }


    public static String getVersion() {
        return String.format("%s-%s", getVersionName(), getVersionCode());
    }


}
