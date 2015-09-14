package com.couchbase.lite.util;

/**
 * Created by hideki on 9/14/15.
 */
public class CountDown {
    private int count = 0;

    public CountDown(int count) {
        this.count = count;
    }

    public int countDown() {
        return --count;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "CountDown{" +
                "count=" + count +
                '}';
    }
}
