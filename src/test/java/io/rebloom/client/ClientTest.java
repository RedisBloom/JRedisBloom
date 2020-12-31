package io.rebloom.client;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Mark Nunberg
 */
public class ClientTest {
    static final int port;
    static {
        String tmpPort = System.getenv("REBLOOM_TEST_PORT");
        if (tmpPort != null && !tmpPort.isEmpty()) {
            port = Integer.parseInt(tmpPort);
        } else {
            port = 6379;
        }
    }

    private Client cl = null;

    @Before
    public void clearDb() {
        cl = new Client("localhost", port);
        cl._conn().flushDB();
    }
    
    @Test
    public void createWithPool() {
      Client refClient;
      try(Client client = new Client(new JedisPool("localhost", port))){
        refClient = client;
        client.createFilter("createBloom", 100, 0.001);
        assertTrue(client.delete("createBloom"));
      }
      assertThrows(JedisException.class, () -> refClient.createFilter("myBloom", 100, 0.001));
    }

    @Test
    public void reserveBasic() {
        cl.createFilter("myBloom", 100, 0.001);
        assertTrue(cl.add("myBloom", "val1"));
        assertTrue(cl.exists("myBloom", "val1"));
        assertFalse(cl.exists("myBloom", "val2"));
    }

    @Test(expected = JedisException.class)
    public void reserveValidateZeroCapacity() {
        cl.createFilter("myBloom", 0, 0.001);
    }

    @Test(expected = JedisException.class)
    public void reserveValidateZeroError() {
        cl.createFilter("myBloom", 100, 0);
    }

    @Test(expected = JedisException.class)
    public void reserveAlreadyExists() {
        cl.createFilter("myBloom", 100, 0.1);
        cl.createFilter("myBloom", 100, 0.1);
    }

    @Test
    public void addExistsString() {
        assertTrue(cl.add("newFilter", "foo"));
        assertTrue(cl.exists("newFilter", "foo"));
        assertFalse(cl.exists("newFilter", "bar"));
        assertFalse(cl.add("newFilter", "foo"));
    }

    @Test
    public void addExistsByte() {
        assertTrue(cl.add("newFilter", "foo".getBytes()));
        assertFalse(cl.add("newFilter", "foo".getBytes()));
        assertTrue(cl.exists("newFilter", "foo".getBytes()));
        assertFalse(cl.exists("newFilter", "bar".getBytes()));
    }

    @Test
    public void testExistsNonExist() {
      assertFalse(cl.exists("nonExist", "foo"));
    }

    @Test
    public void addExistsMulti() {
        boolean rv[] = cl.addMulti("newFilter", "foo", "bar", "baz");
        assertEquals(3, rv.length);
        for (boolean i : rv) {
            assertTrue(i);
        }

        rv = cl.addMulti("newFilter", "newElem", "bar", "baz");
        assertEquals(3, rv.length);
        assertTrue(rv[0]);
        assertFalse(rv[1]);
        assertFalse(rv[2]);

        // Try with bytes
        rv = cl.addMulti("newFilter", new byte[]{1}, new byte[]{2}, new byte[]{3});
        assertEquals(3, rv.length);
        for (boolean i : rv) {
            assertTrue(i);
        }

        rv = cl.addMulti("newFilter", new byte[]{0}, new byte[]{3});
        assertEquals(2, rv.length);
        assertTrue(rv[0]);
        assertFalse(rv[1]);

        rv = cl.existsMulti("newFilter", new byte[]{0}, new byte[]{1}, new byte[]{2}, new byte[]{3}, new byte[]{5});
        assertEquals(5, rv.length);
        assertTrue(rv[0]);
        assertTrue(rv[1]);
        assertTrue(rv[2]);
        assertTrue(rv[3]);
        assertFalse(rv[4]);
    }

    @Test
    public void testExample() {
        Client client = cl;
        // Simple bloom filter using default module settings
        client.add("simpleBloom", "Mark");
        // Does "Mark" now exist?
        client.exists("simpleBloom", "Mark"); // true
        client.exists("simpleBloom", "Farnsworth"); // False

        // If you have a long list of items to check/add, you can use the
        // "multi" methods

        client.addMulti("simpleBloom", "foo", "bar", "baz", "bat", "bag");

        // Check if they exist:
        boolean[] rv = client.existsMulti("simpleBloom", "foo", "bar", "baz", "bat", "Mark", "nonexist");
        // All items except the last one will be 'true'
        assertEquals(Arrays.toString(new boolean[]{true, true, true, true, true, false}), Arrays.toString(rv));

        // Reserve a "customized" bloom filter
        client.createFilter("specialBloom", 10000, 0.0001);
        client.add("specialBloom", "foo");
    }
   
    @Test
    public void createTopKFilter() {
      cl.topkCreateFilter("aaa", 30, 2000, 7, 0.925);
      
      assertEquals(Arrays.asList(null, null), cl.topkAdd("aaa", "bb", "cc"));
      
      assertEquals(Arrays.asList(true, false, true), cl.topkQuery("aaa", "bb", "gg", "cc"));
      
      assertEquals(Arrays.asList(1L, 0L, 1L), cl.topkCount("aaa", "bb", "gg", "cc"));

      assertTrue( cl.topkList("aaa").stream().allMatch( s -> Arrays.asList("bb", "cc").contains(s) || s == null));
      
      assertEquals(null, cl.topkIncrBy("aaa", "ff", 10));
      
      assertTrue( cl.topkList("aaa").stream().allMatch( s -> Arrays.asList("bb", "cc", "ff").contains(s) || s == null));
    }

    @Test
    public void testInsert() {
        cl.insert("b1", new InsertOptions().capacity(1L), "1");
        assertTrue(cl.exists("b1", "1"));

        // returning an error if the filter does not already exist
        Exception exception = assertThrows(JedisDataException.class, () -> cl.insert("b2", new InsertOptions().nocreate(), "1"));
        assertEquals("ERR not found", exception.getMessage());

        cl.insert("b3", new InsertOptions().capacity(1L).error(0.0001), "2");
        assertTrue(cl.exists("b3", "2"));
    }

    @Test
    public void testInfo() {
        cl.insert("test_info", new InsertOptions().capacity(1L), "1");
        Map<String, Object> info = cl.info("test_info");
        assertEquals("1", info.get("Number of items inserted").toString());

        // returning an error if the filter does not already exist
        Exception exception = assertThrows(JedisDataException.class, () -> cl.info("not_exist"));
        assertEquals("ERR not found", exception.getMessage());
    }
}