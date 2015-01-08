package com.couchbase.lite.support;

import com.couchbase.lite.util.Log;

public class Version {

    public static final String VERSION;

    private static final String VERSION_NAME="%VERSION_NAME%";  // replaced during build process
    private static final String VERSION_CODE="%VERSION_CODE%";  // replaced during build process

    static {
        int versCode=getVersionCode();
        if (versCode==-1) {
            VERSION = String.format("%s-%s", getVersionName(), getVersionCode());
        } else{
            VERSION = String.format("%s", getVersionName());
        }
    }

    public static String getVersionName() {
        if (VERSION_NAME.contains("VERSION_NAME")) {
            return "devbuild";
        }
        return VERSION_NAME;
    }

    public static int getVersionCode() {
        if (VERSION_CODE.contains("VERSION_CODE")) {
            return 0;
        }
        try {
            int versionCode = Integer.parseInt(VERSION_CODE);
            return versionCode;
        } catch (NumberFormatException e) {
            Log.e(Log.TAG, "Cannot parse version code: %s", VERSION_CODE);
        }
        return -1;
    }


    public static String getVersion() {
        return String.format("%s-%s", getVersionName(), getVersionCode());
    }


}
