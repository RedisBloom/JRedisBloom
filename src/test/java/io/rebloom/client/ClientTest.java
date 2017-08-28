package io.rebloom.client;

import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.*;


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

    Client cl = null;

    @Before
    public void clearDb() throws Exception {
        cl = new Client("localhost", port);
        cl._conn().flushDB();
    }

    @Test
    public void reserveBasic() throws Exception {
        cl.createFilter("myBloom", 100, 0.001);
        assertTrue(cl.add("myBloom", "val1"));
        assertTrue(cl.exists("myBloom", "val1"));
        assertFalse(cl.exists("myBloom", "val2"));
    }

    @Test(expected = JedisException.class)
    public void reserveValidateZeroCapacity() throws Exception {
        cl.createFilter("myBloom", 0, 0.001);
    }

    @Test(expected = JedisException.class)
    public void reserveValidateZeroError() throws Exception {
        cl.createFilter("myBloom", 100, 0);
    }

    @Test(expected = JedisException.class)
    public void reserveAlreadyExists() throws Exception {
        cl.createFilter("myBloom", 100, 0.1);
        cl.createFilter("myBloom", 100, 0.1);
    }

    @Test
    public void addExistsString() throws Exception {
        assertTrue(cl.add("newFilter", "foo"));
        assertTrue(cl.exists("newFilter", "foo"));
        assertFalse(cl.exists("newFilter", "bar"));
        assertFalse(cl.add("newFilter", "foo"));
    }

    @Test
    public void addExistsByte() throws Exception {
        assertTrue(cl.add("newFilter", "foo".getBytes()));
        assertFalse(cl.add("newFilter", "foo".getBytes()));
        assertTrue(cl.exists("newFilter", "foo".getBytes()));
        assertFalse(cl.exists("newFilter", "bar".getBytes()));
    }

    @Test(expected = JedisException.class)
    public void testExistsNonExist() throws Exception {
        cl.exists("nonExist", "foo");
    }

    @Test
    public void addExistsMulti() throws Exception {
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
    public void testExample() throws Exception {
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
        boolean[] rv = client.existsMulti("simpleBloom", "foo", "bar", "baz", "bat", "mark", "nonexist");
        // All items except the last one will be 'true'


        // Reserve a "customized" bloom filter
        client.createFilter("specialBloom", 10000, 0.0001);
        client.add("specialBloom", "foo");
    }

}