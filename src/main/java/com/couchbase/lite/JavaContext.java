package com.couchbase.lite;

import java.io.File;

/**
 * This is the default couchbase lite context when running "portable java" (eg, non-Android platforms
 * such as Linux or OSX).
 *
 * If you are running on Android, you will want to use AndroidContext instead.  At the time of writing,
 * the AndroidContext is currently not available in the javadocs due to an issue in our build
 * infrastructure.
 */
public class JavaContext implements Context {

    private String subdir;

    public JavaContext(String subdir) {
        this.subdir = subdir;
    }

    public JavaContext() {
        this.subdir = "cblite";
    }

    @Override
    public File getFilesDir() {
        return new File(getRootDirectory(), subdir);
    }

    @Override
    public void setNetworkReachabilityManager(NetworkReachabilityManager networkReachabilityManager) {

    }

    @Override
    public NetworkReachabilityManager getNetworkReachabilityManager() {
        return new FakeNetworkReachabilityManager();
    }

    public File getRootDirectory() {
        String rootDirectoryPath = System.getProperty("user.dir");
        File rootDirectory = new File(rootDirectoryPath);
        rootDirectory = new File(rootDirectory, "data/data/com.couchbase.lite.test/files");

        return rootDirectory;
    }

    class FakeNetworkReachabilityManager extends NetworkReachabilityManager {
        @Override
        public void startListening() {

        }

        @Override
        public void stopListening() {

        }
    }

}
