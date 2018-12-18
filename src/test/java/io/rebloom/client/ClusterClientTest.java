package io.rebloom.client;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * @author TommyYang on 2018/12/17
 */
public class ClusterClientTest {


    ClusterClient ccl = null;

    @Before
    public void newCCL() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort("localhost", 6379));
//        jedisClusterNodes.add(new HostAndPort("localhost", 6380));
//        jedisClusterNodes.add(new HostAndPort("localhost", 6381));
//        jedisClusterNodes.add(new HostAndPort("localhost", 6382));
        ccl = new ClusterClient(jedisClusterNodes);
    }

    @Test
    public void reserveBasic() {
        boolean res = ccl.createFilter("myBloom", 100, 0.001);
        assertTrue(ccl.add("myBloom", "val1"));
        assertTrue(ccl.exists("myBloom", "val1"));
        assertFalse(ccl.exists("myBloom", "val2"));
        assertTrue(res);
    }

    @Test
    public void delete(){
        boolean res = ccl.delete("newFilter");
        assertTrue(res);
    }

    @Test(expected = JedisException.class)
    public void reserveValidateZeroCapacity() {
        ccl.createFilter("myBloom", 0, 0.001);
    }

    @Test(expected = JedisException.class)
    public void reserveValidateZeroError() {
        ccl.createFilter("myBloom", 100, 0);
    }

    @Test(expected = JedisException.class)
    public void reserveAlreadyExists() {
        ccl.createFilter("myBloom", 100, 0.1);
        ccl.createFilter("myBloom", 100, 0.1);
    }

    @Test
    public void addExistsString() {
        TestCase.assertTrue(ccl.add("newFilter", "foo"));
        TestCase.assertTrue(ccl.exists("newFilter", "foo"));
        TestCase.assertFalse(ccl.exists("newFilter", "bar"));
        TestCase.assertFalse(ccl.add("newFilter", "foo"));
    }

    @Test
    public void addExistsByte() {
        TestCase.assertTrue(ccl.add("newFilter", "foo".getBytes()));
        TestCase.assertFalse(ccl.add("newFilter", "foo".getBytes()));
        TestCase.assertTrue(ccl.exists("newFilter", "foo".getBytes()));
        TestCase.assertFalse(ccl.exists("newFilter", "bar".getBytes()));
    }

    @Test
    public void addExistsMulti() {
        boolean rv[] = ccl.addMulti("newFilter", "foo", "bar", "baz");
        assertEquals(3, rv.length);
        for (boolean i : rv) {
            TestCase.assertTrue(i);
        }

        rv = ccl.addMulti("newFilter", "newElem", "bar", "baz");
        assertEquals(3, rv.length);
        TestCase.assertTrue(rv[0]);
        TestCase.assertFalse(rv[1]);
        TestCase.assertFalse(rv[2]);

        // Try with bytes
        rv = ccl.addMulti("newFilter", new byte[]{1}, new byte[]{2}, new byte[]{3});
        assertEquals(3, rv.length);
        for (boolean i : rv) {
            TestCase.assertTrue(i);
        }

        rv = ccl.addMulti("newFilter", new byte[]{0}, new byte[]{3});
        assertEquals(2, rv.length);
        TestCase.assertTrue(rv[0]);
        TestCase.assertFalse(rv[1]);

        rv = ccl.existsMulti("newFilter", new byte[]{0}, new byte[]{1}, new byte[]{2}, new byte[]{3}, new byte[]{5});
        assertEquals(5, rv.length);
        TestCase.assertTrue(rv[0]);
        TestCase.assertTrue(rv[1]);
        TestCase.assertTrue(rv[2]);
        TestCase.assertTrue(rv[3]);
        TestCase.assertFalse(rv[4]);
    }

}
