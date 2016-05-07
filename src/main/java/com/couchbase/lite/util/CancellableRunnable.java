package com.couchbase.lite.util;

/**
 * Created by hideki on 5/5/16.
 */
public interface CancellableRunnable extends Runnable{
    void cancel();
}

