package com.bigfast.cache;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An OffHeap String Cache with a LRU expiration(eviction) policy.
 * 
 * Note: The cache needs to perform its own memory management as it is outside the JVM Heap and not
 * subject to Garbage Collection. On the otherhand, it avoids potential pitfalls observed during GC,
 * especially of large heaps like stop the world sweeps which affect low latency applications.
 *
 * @author isyed
 * @version 0.1
 */
public final class OffHeapCache {

    private final int maxLength;//Max size of a string that can be put into the cache
    private final LinkedHashMap<String, CharBuffer> cache;//LRU cache
    private final LinkedList<CharBuffer> freeList;//Used to prevent memory fragmentation
    private final Lock lock = new ReentrantLock();
    private static final OffHeapCache OFFHEAP = new OffHeapCache(120000, 140);//Using default values

    public OffHeapCache(int size, int maxLength) {
        this.maxLength = maxLength;
        //Cache Size is fixed in the beginning. True => maintains access order
        cache = new LinkedHashMap<String, CharBuffer>(size, .75f, true);
        freeList = new LinkedList<CharBuffer>();
        for (int i = 0; i < size; i++) {
            //Direct Allocation using ByteBuffer, then accessed as characters
            CharBuffer buf = ByteBuffer.allocateDirect(2 * maxLength).asCharBuffer();
            freeList.add(buf);
        }
    }

    public static final OffHeapCache getOffHeapCache() {
        return OFFHEAP;
    }

    /**
     * Stores data into cache
     * @param key String key
     * @param value String to store in cache
     */
    public String put(String key, String value) {
        if (value.length() > maxLength) {
            throw new IllegalArgumentException("string too long: " + value.length());
        }
        lock.lock();
        try {
            CharBuffer buf = cache.get(key);//checks for duplicates
            if (buf == null) {
                buf = (freeList.size() > 0) ? freeList.removeFirst() : null;
            }
            if (buf == null) {
                Entry<String, CharBuffer> eldest = cache.entrySet().iterator().next();//Map iteration is in LRU order
                buf = eldest.getValue();
                cache.remove(eldest.getKey());
            }
            buf.clear();
            buf.put(value);
            buf.limit(value.length());
            cache.put(key, buf);
            return value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves data from cache
     * @param key String
     * @return String if present in cache, otherwise null
     */
    public String get(String key) {
        lock.lock();
        try {
            CharBuffer buf = cache.get(key);
            if (buf == null) {
                return null;
            }
            buf.position(0);
            return buf.toString();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Moves data directly from cache to output - instead of copying to heap
     * @param key
     * @param writer
     * @return True if successful, otherwise false
     * @throws java.io.IOException
     */
    public boolean write(String key, Writer writer)
            throws IOException {
        lock.lock();
        try {
            CharBuffer buf = cache.get(key);
            if ((buf == null) || (buf.limit() == 0)) {
                return false;
            }
            // TODO: use a (small) char[]
            buf.position(0);
            for (int i = 0; i < buf.limit(); i++) {
                writer.write(buf.get(i));
            }
            return true;
        } finally {
            lock.unlock();
        }
    }
}
