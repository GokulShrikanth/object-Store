package com.objstore.common;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V extends Serializable> extends LinkedHashMap<K, V> {
    private final int maxEntries;

    public LRUCache(int maxEntries) {
        super(16, 0.75f, true);
        this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxEntries;
    }
}