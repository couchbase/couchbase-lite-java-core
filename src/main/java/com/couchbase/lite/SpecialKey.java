package com.couchbase.lite;

/**
 * CBLSpecialKey.h/CBLSpecialKey.m
 *
 * Created by hideki on 8/19/15.
 */
public class SpecialKey {
    private String text;

    public SpecialKey(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        if (text != null)
            return "SpecialKey{" +
                    "text='" + text + '\'' +
                    '}';
        else
            return "SpecialKey{}";
    }
}
