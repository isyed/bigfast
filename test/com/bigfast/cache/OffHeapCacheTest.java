package com.bigfast.cache;

import java.io.PrintWriter;
import java.io.Writer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author isyed
 */
public class OffHeapCacheTest {

    private static OffHeapCache cache;

    public OffHeapCacheTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        cache = new OffHeapCache(10, 10);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of put method, of class OffHeapCache.
     */
    @Test
    public void testPut() {
        System.out.println("put");
        String key = "a";
        String value = "first";

        cache.put(key, value);
    }

    /**
     * Test of get method, of class OffHeapCache.
     */
    @Test
    public void testGet() {
        System.out.println("get");
        String key = "a";
        String expResult = "first";
        String result = cache.get(key);
        assertEquals(expResult, result);
    }

    /**
     * Test of write method, of class OffHeapCache.
     */
    @Test
    public void testWrite() throws Exception {
        System.out.println("write");
        String key = "a";
        Writer writer = new PrintWriter(System.out);
        boolean expResult = true;
        boolean result = cache.write(key, writer);
        writer.flush();
        assertEquals(expResult, result);

    }
}
