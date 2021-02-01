package io.rebloom.client;

import junit.framework.TestCase;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.ClusterReset;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertThrows;


/**
 * @author TommyYang on 2018/12/17
 */
public class ClusterClientTest {


    private ClusterClient ccl = null;

    private final static HostAndPort nodeInfo1 = new HostAndPort("127.0.0.1", 7379);
    private final static HostAndPort nodeInfo2 = new HostAndPort("127.0.0.1", 7380);
    private final static HostAndPort nodeInfo3 = new HostAndPort("127.0.0.1", 7381);
    private static Jedis node1;
    private static Jedis node2;
    private static Jedis node3;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        node1 = new Jedis(nodeInfo1);
        node1.flushAll();

        node2 = new Jedis(nodeInfo2);
        node2.flushAll();

        node3 = new Jedis(nodeInfo3);
        node3.flushAll();

        // add nodes to cluster
        node1.clusterMeet("127.0.0.1", nodeInfo2.getPort());
        node1.clusterMeet("127.0.0.1", nodeInfo3.getPort());

        // split available slots across the three nodes
        int slotsPerNode = JedisCluster.HASHSLOTS / 3;
        int[] node1Slots = new int[slotsPerNode];
        int[] node2Slots = new int[slotsPerNode + 1];
        int[] node3Slots = new int[slotsPerNode];
        for (int i = 0, slot1 = 0, slot2 = 0, slot3 = 0; i < JedisCluster.HASHSLOTS; i++) {
            if (i < slotsPerNode) {
                node1Slots[slot1++] = i;
            } else if (i > slotsPerNode * 2) {
                node3Slots[slot3++] = i;
            } else {
                node2Slots[slot2++] = i;
            }
        }

        node1.clusterAddSlots(node1Slots);
        node2.clusterAddSlots(node2Slots);
        node3.clusterAddSlots(node3Slots);

        waitForClusterReady(node1, node2, node3);
    }

    @AfterClass
    public static void cleanUp() {
        node1.flushDB();
        node2.flushDB();
        node3.flushDB();
        node1.clusterReset(ClusterReset.SOFT);
        node2.clusterReset(ClusterReset.SOFT);
        node3.clusterReset(ClusterReset.SOFT);
    }

    @Before
    public void newCCL() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(new HostAndPort("127.0.0.1", 7379));
        ccl = new ClusterClient(jedisClusterNodes);

        node1.flushDB();
        node2.flushDB();
        node3.flushDB();
    }

    private static void waitForClusterReady(Jedis... nodes) throws InterruptedException {
        boolean clusterOk = false;
        while (!clusterOk) {
            boolean isOk = true;
            for (Jedis node : nodes) {
                if (!node.clusterInfo().split("\n")[0].contains("ok")) {
                    isOk = false;
                    break;
                }
            }

            if (isOk) {
                clusterOk = true;
            }

            Thread.sleep(50);
        }
    }

    @Test
    public void reserveBasic() {
        assertTrue(ccl.createFilter("myBloom", 100, 0.001));
        assertTrue(ccl.add("myBloom", "val1"));
        assertTrue(ccl.exists("myBloom", "val1"));
        assertFalse(ccl.exists("myBloom", "val2"));
    }

    @Test
    public void delete() {
        assertTrue(ccl.createFilter("newFilter", 100, 0.001));
        assertTrue(ccl.delete("newFilter"));
    }

    @Test(expected = JedisException.class)
    public void reserveValidateZeroCapacity() {
        ccl.createFilter("myBloom", 0, 0.001);
    }

    @Test(expected = JedisException.class)
    public void reserveValidateZeroError() {
        ccl.createFilter("myBloom", 100, 0);
    }

    @Test
    public void reserveAlreadyExists() {
        assertTrue(ccl.createFilter("myBloom", 100, 0.1));
        assertThrows(JedisException.class, () -> ccl.createFilter("myBloom", 100, 0.1));
    }

    @Test
    public void addExistsString() {
        assertTrue(ccl.add("newFilter", "foo"));
        assertTrue(ccl.exists("newFilter", "foo"));
        assertFalse(ccl.exists("newFilter", "bar"));
        assertFalse(ccl.add("newFilter", "foo"));
    }

    @Test
    public void addExistsByte() {
        assertTrue(ccl.add("newFilter", "foo".getBytes()));
        assertFalse(ccl.add("newFilter", "foo".getBytes()));
        assertTrue(ccl.exists("newFilter", "foo".getBytes()));
        assertFalse(ccl.exists("newFilter", "bar".getBytes()));
    }

    @Test
    public void addExistsMulti() {
        boolean rv[] = ccl.addMulti("newFilter", "foo", "bar", "baz");
        assertEquals(3, rv.length);
        for (boolean i : rv) {
            assertTrue(i);
        }

        rv = ccl.addMulti("newFilter", "newElem", "bar", "baz");
        assertEquals(3, rv.length);
        assertTrue(rv[0]);
        assertFalse(rv[1]);
        assertFalse(rv[2]);

        // Try with bytes
        rv = ccl.addMulti("newFilter", new byte[]{1}, new byte[]{2}, new byte[]{3});
        assertEquals(3, rv.length);
        for (boolean i : rv) {
            TestCase.assertTrue(i);
        }

        rv = ccl.addMulti("newFilter", new byte[]{0}, new byte[]{3});
        assertEquals(2, rv.length);
        assertTrue(rv[0]);
        assertFalse(rv[1]);

        rv = ccl.existsMulti("newFilter", new byte[]{0}, new byte[]{1}, new byte[]{2}, new byte[]{3}, new byte[]{5});
        assertEquals(5, rv.length);
        assertTrue(rv[0]);
        assertTrue(rv[1]);
        assertTrue(rv[2]);
        assertTrue(rv[3]);
        assertFalse(rv[4]);
    }

    @Test
    public void createTopKFilter() {
        ccl.topkCreateFilter("aaa", 30, 2000, 7, 0.925);
        ccl.topkCreateFilter("zzz", 40, 2000, 7, 0.925);
        ccl.topkCreateFilter("yzx", 50, 2000, 7, 0.925);

        assertEquals(Arrays.asList(null, null), ccl.topkAdd("aaa", "bb", "cc"));

        assertEquals(Arrays.asList(true, false, true), ccl.topkQuery("aaa", "bb", "gg", "cc"));

        assertEquals(Arrays.asList(1L, 0L, 1L), ccl.topkCount("aaa", "bb", "gg", "cc"));

        assertTrue(ccl.topkList("aaa").stream().allMatch(s -> Arrays.asList("bb", "cc").contains(s) || s == null));

        assertEquals(null, ccl.topkIncrBy("aaa", "ff", 10));

        assertTrue(ccl.topkList("aaa").stream().allMatch(s -> Arrays.asList("bb", "cc", "ff").contains(s) || s == null));
    }

    @Test
    public void testInsert() {
        ccl.insert("b1", new InsertOptions().capacity(1L), "1");
        assertTrue(ccl.exists("b1", "1"));

        // returning an error if the filter does not already exist
        Exception exception = assertThrows(JedisDataException.class, () -> ccl.insert("b2", new InsertOptions().nocreate(), "1"));
        assertEquals("ERR not found", exception.getMessage());

        ccl.insert("b3", new InsertOptions().capacity(1L).error(0.0001), "2");
        assertTrue(ccl.exists("b3", "2"));
    }

    @Test
    public void testInfo() {
        ccl.insert("test_info", new InsertOptions().capacity(1L), "1");
        Map<String, Object> info = ccl.info("test_info");
        assertEquals("1", info.get("Number of items inserted").toString());

        // returning an error if the filter does not already exist
        Exception exception = assertThrows(JedisDataException.class, () -> ccl.info("not_exist"));
        assertEquals("ERR not found", exception.getMessage());
    }
}
