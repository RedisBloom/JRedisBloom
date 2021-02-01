package io.rebloom.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.exceptions.JedisException;

/**
 * Tests for the Count-Min-Sketch Implementation
 *
 */
public class CMSTests {
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
  public void clearDb() {
    cl = new Client("localhost", port);
    cl._conn().flushDB();
  }

  @Test
  public void testInitByDim() {
    cl.cmsInitByDim("cms1", 16L, 4L);
    Map<String, Long> info = cl.cmsInfo("cms1");
    assertEquals(16L, info.get("width").longValue());
    assertEquals(4L, info.get("depth").longValue());
    assertEquals(0L, info.get("count").longValue());
  }

  @Test
  public void testInitByProb() {
    cl.cmsInitByProb("cms2", 0.01, 0.01);
    Map<String, Long> info = cl.cmsInfo("cms2");
    assertEquals(200L, info.get("width").longValue());
    assertEquals(7L, info.get("depth").longValue());
    assertEquals(0L, info.get("count").longValue());
  }

  @Test
  public void testKeyAlreadyExists() {
    cl.cmsInitByDim("dup", 16L, 4L);
    JedisException thrown = assertThrows(JedisException.class, () -> {
      cl.cmsInitByDim("dup", 8L, 6L);
    });
    assertEquals("CMS: key already exists", thrown.getMessage());
  }

  @Test
  public void testIncrBy() {
    cl.cmsInitByDim("cms3", 1000L, 5L);
    long resp = cl.cmsIncrBy("cms3", "foo", 5L);
    assertEquals(5L, resp);

    Map<String, Long> info = cl.cmsInfo("cms3");
    assertEquals(1000L, info.get("width").longValue());
    assertEquals(5L, info.get("depth").longValue());
    assertEquals(5L, info.get("count").longValue());
  }

  @Test
  public void testIncrByMultipleArgs() {
    cl.cmsInitByDim("cms4", 1000L, 5L);
    cl.cmsIncrBy("cms4", "foo", 5L);

    Map<String, Long> itemIncrements = new HashMap<String, Long>();
    itemIncrements.put("foo", 5L);
    itemIncrements.put("bar", 15L);

    List<Long> resp = cl.cmsIncrBy("cms4", itemIncrements);
    assertArrayEquals(new Long[] { 15L, 10L }, resp.toArray(new Long[0]));

    Map<String, Long> info = cl.cmsInfo("cms4");
    assertEquals(1000L, info.get("width").longValue());
    assertEquals(5L, info.get("depth").longValue());
    assertEquals(25L, info.get("count").longValue());
  }

  @Test
  public void testQuery() {
    cl.cmsInitByDim("cms5", 1000L, 5L);

    Map<String, Long> itemIncrements = new HashMap<String, Long>();
    itemIncrements.put("foo", 10L);
    itemIncrements.put("bar", 15L);

    cl.cmsIncrBy("cms5", itemIncrements);

    List<Long> resp = cl.cmsQuery("cms5", "foo", "bar");
    assertArrayEquals(new Long[] { 10L, 15L }, resp.toArray(new Long[0]));
  }

  @Test
  public void testMerge() {
    cl.cmsInitByDim("A", 1000L, 5L);
    cl.cmsInitByDim("B", 1000L, 5L);
    cl.cmsInitByDim("C", 1000L, 5L);

    Map<String, Long> aValues = new HashMap<String, Long>();
    aValues.put("foo", 5L);
    aValues.put("bar", 3L);
    aValues.put("baz", 9L);

    cl.cmsIncrBy("A", aValues);

    Map<String, Long> bValues = new HashMap<String, Long>();
    bValues.put("foo", 2L);
    bValues.put("bar", 3L);
    bValues.put("baz", 1L);

    cl.cmsIncrBy("B", bValues);

    List<Long> q1 = cl.cmsQuery("A", "foo", "bar", "baz");
    assertArrayEquals(new Long[] { 5L, 3L, 9L }, q1.toArray(new Long[0]));

    List<Long> q2 = cl.cmsQuery("B", "foo", "bar", "baz");
    assertArrayEquals(new Long[] { 2L, 3L, 1L }, q2.toArray(new Long[0]));

    cl.cmsMerge("C", "A", "B");

    List<Long> q3 = cl.cmsQuery("C", "foo", "bar", "baz");
    assertArrayEquals(new Long[] { 7L, 6L, 10L }, q3.toArray(new Long[0]));

    Map<String, Long> keysAndWeights = new HashMap<String, Long>();
    keysAndWeights.put("A", 1L);
    keysAndWeights.put("B", 2L);

    cl.cmsMerge("C", keysAndWeights);

    List<Long> q4 = cl.cmsQuery("C", "foo", "bar", "baz");
    assertArrayEquals(new Long[] { 9L, 9L, 11L }, q4.toArray(new Long[0]));

    keysAndWeights.clear();
    keysAndWeights.put("A", 2L);
    keysAndWeights.put("B", 3L);

    cl.cmsMerge("C", keysAndWeights);

    List<Long> q5 = cl.cmsQuery("C", "foo", "bar", "baz");
    assertArrayEquals(new Long[] { 16L, 15L, 21L }, q5.toArray(new Long[0]));
  }

}