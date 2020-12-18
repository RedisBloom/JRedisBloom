package io.rebloom.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.exceptions.JedisDataException;

import io.rebloom.client.cf.IteratorDataPair;

/**
 * Tests for the Cuckoo Filter Implementation
 *
 */
public class CuckooTests {
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
  public void testReservationCapacityOnly() {
    cl.cfCreate("cuckoo1", 10);
    Map<String, Long> info = cl.cfInfo("cuckoo1");

    assertEquals(8L, info.get("Number of buckets").longValue());
    assertEquals(0L, info.get("Number of items inserted").longValue());
    assertEquals(72L, info.get("Size").longValue());
    assertEquals(1L, info.get("Expansion rate").longValue());
    assertEquals(1L, info.get("Number of filters").longValue());
    assertEquals(2L, info.get("Bucket size").longValue());
    assertEquals(20L, info.get("Max iterations").longValue());
    assertEquals(0L, info.get("Number of items deleted").longValue());
  }

  @Test
  public void testReservationCapacityAndBucketSize() {
    cl.cfCreate("cuckoo2", 200, 10);
    Map<String, Long> info = cl.cfInfo("cuckoo2");

    assertEquals(32L, info.get("Number of buckets").longValue());
    assertEquals(0L, info.get("Number of items inserted").longValue());
    assertEquals(376L, info.get("Size").longValue());
    assertEquals(1L, info.get("Expansion rate").longValue());
    assertEquals(1L, info.get("Number of filters").longValue());
    assertEquals(10L, info.get("Bucket size").longValue());
    assertEquals(20L, info.get("Max iterations").longValue());
    assertEquals(0L, info.get("Number of items deleted").longValue());
  }

  @Test
  public void testReservationCapacityAndBucketSizeAndMaxIterations() {
    cl.cfCreate("cuckoo3", 200, 10, 20);
    Map<String, Long> info = cl.cfInfo("cuckoo3");

    assertEquals(32L, info.get("Number of buckets").longValue());
    assertEquals(0L, info.get("Number of items inserted").longValue());
    assertEquals(376L, info.get("Size").longValue());
    assertEquals(1L, info.get("Expansion rate").longValue());
    assertEquals(1L, info.get("Number of filters").longValue());
    assertEquals(10L, info.get("Bucket size").longValue());
    assertEquals(20L, info.get("Max iterations").longValue());
    assertEquals(0L, info.get("Number of items deleted").longValue());
  }

  @Test
  public void testReservationAllParams() {
    cl.cfCreate("cuckoo4", 200, 10, 20, 4);
    Map<String, Long> info = cl.cfInfo("cuckoo4");

    assertEquals(32L, info.get("Number of buckets").longValue());
    assertEquals(0L, info.get("Number of items inserted").longValue());
    assertEquals(376L, info.get("Size").longValue());
    assertEquals(4L, info.get("Expansion rate").longValue());
    assertEquals(1L, info.get("Number of filters").longValue());
    assertEquals(10L, info.get("Bucket size").longValue());
    assertEquals(20L, info.get("Max iterations").longValue());
    assertEquals(0L, info.get("Number of items deleted").longValue());
  }

  @Test
  public void testAdd() {
    cl.cfCreate("cuckoo5", 64000);
    assertTrue(cl.cfAdd("cuckoo5", "test"));

    Map<String, Long> info = cl.cfInfo("cuckoo5");
    assertEquals(1L, info.get("Number of items inserted").longValue());
  }

  @Test
  public void testAddNxItemDoesExist() {
    cl.cfCreate("cuckoo6", 64000);
    assertTrue(cl.cfAddNx("cuckoo6", "filter"));
  }

  @Test
  public void testAddNxItemExists() {
    cl.cfCreate("cuckoo7", 64000);
    cl.cfAdd("cuckoo7", "filter");
    assertFalse(cl.cfAddNx("cuckoo7", "filter"));
  }

  @Test
  public void testInsert() {
    assertArrayEquals( //
        new Boolean[] { true }, //
        cl.cfInsert("cuckoo8", "foo").toArray(new Boolean[0]) //
    );
  }

  @Test
  public void testInsertWithCapacity() {
    assertArrayEquals( //
        new Boolean[] { true }, //
        cl.cfInsert("cuckoo9", 1000, "foo").toArray(new Boolean[0]) //
    );
  }

  @Test
  public void testInsertNoCreateFilterDoesNotExist() {
    Exception ex = assertThrows(JedisDataException.class, () -> {
      cl.cfInsertNoCreate("cuckoo10", "foo", "bar");
    });
    assertTrue(ex.getMessage().contains("ERR not found"));
  }

  @Test
  public void testInsertNoCreateFilterExists() {
    cl.cfInsert("cuckoo11", "bar");
    assertArrayEquals( //
        new Boolean[] { true, true }, //
        cl.cfInsertNoCreate("cuckoo11", "foo", "bar").toArray(new Boolean[0]) //
    );
  }

  @Test
  public void testInsertNx() {
    assertArrayEquals( //
        new Boolean[] { true }, //
        cl.cfInsertNx("cuckoo12", "bar").toArray(new Boolean[0]) //
    );
  }

  @Test
  public void testInsertNxWithCapacity() {
    cl.cfInsertNx("cuckoo13", "bar");
    assertArrayEquals( //
        new Boolean[] { false }, //
        cl.cfInsertNx("cuckoo13", 1000, "bar").toArray(new Boolean[0]) //
    );
  }

  @Test
  public void testInsertNxMultiple() {
    cl.cfInsertNx("cuckoo14", "foo");
    cl.cfInsertNx("cuckoo14", "bar");
    assertArrayEquals(//
        new Boolean[] { false, false, true }, //
        cl.cfInsertNx("cuckoo14", "foo", "bar", "baz").toArray(new Boolean[0])//
    );
  }

  @Test
  public void testInsertNxNoCreate() {
    Exception ex = assertThrows(JedisDataException.class, () -> {
      cl.cfInsertNxNoCreate("cuckoo15", "foo", "bar");
    });
    assertTrue(ex.getMessage().contains("ERR not found"));
  }

  @Test
  public void testExistsItemDoesntExist() {
    assertFalse(cl.cfExists("cuckoo16", "foo"));
  }

  @Test
  public void testExistsItemExists() {
    cl.cfInsert("cuckoo17", "foo");
    assertTrue(cl.cfExists("cuckoo17", "foo"));
  }

  @Test
  public void testDeleteItemDoesntExist() {
    cl.cfInsert("cuckoo8", "bar");
    assertFalse(cl.cfDel("cuckoo8", "foo"));
  }

  @Test
  public void testDeleteItemExists() {
    cl.cfInsert("cuckoo18", "foo");
    assertTrue(cl.cfDel("cuckoo18", "foo"));
  }

  @Test
  public void testDeleteFilterDoesNotExist() {
    Exception ex = assertThrows(JedisDataException.class, () -> {
      cl.cfDel("cuckoo19", "foo");
    });
    assertTrue(ex.getMessage().contains("Not found"));
  }

  @Test
  public void testCountFilterDoesNotExist() {
    assertEquals(0L, cl.cfCount("cuckoo20", "filter"));
  }

  @Test
  public void testCountFilterExist() {
    cl.cfInsert("cuckoo21", "foo");
    assertEquals(0L, cl.cfCount("cuckoo21", "filter"));
  }

  @Test
  public void testCountItemExists() {
    cl.cfInsert("cuckoo22", "foo");
    assertEquals(1L, cl.cfCount("cuckoo22", "foo"));
  }

  @Test
  public void testInfoFilterDoesNotExists() {
    Exception ex = assertThrows(JedisDataException.class, () -> {
      cl.cfInfo("cuckoo23");
    });
    assertTrue(ex.getMessage().contains("ERR not found"));
  }

  @Test
  public void testScanDump() {
    cl.cfCreate("cuckoo24", 100, 50);
    cl.cfAdd("cuckoo24", "a");

    long iter = 0;
    List<IteratorDataPair> chunks = new ArrayList<>();
    while (true) {
      IteratorDataPair chunksData = cl.cfScanDump("cuckoo24", iter);
      iter = chunksData.getIteratorValue();
      if (iter == 0) {
        break;
      }
      chunks.add(chunksData);
    }

    cl.delete("cuckoo24");

    // Load it back
    chunks.forEach(chunksData -> cl.cfLoadChunk("cuckoo24", chunksData));

    // check the bucket size
    Map<String, Long> info = cl.cfInfo("cuckoo24");

    // check the retrieved filter properties
    assertEquals(1L, info.get("Number of items inserted").longValue());
    assertEquals(50L, info.get("Bucket size").longValue());
    assertEquals(0L, info.get("Number of items deleted").longValue());

    // check for existing items
    assertTrue(cl.cfExists("cuckoo24", "a"));
  }
  
  @Test
  public void testScanDumpWithIterator() {
    cl.cfCreate("cuckoo25", 100, 50);
    cl.cfAdd("cuckoo25", "b");
    
    List<IteratorDataPair> chunks = new ArrayList<>();
    Iterator<IteratorDataPair> i = cl.cfScanDumpIterator("cuckoo25");
    while (i.hasNext()) {
      chunks.add(i.next());
    }

    cl.delete("cuckoo25");

    // Load it back
    chunks.forEach(chunksData -> {
      cl.cfLoadChunk("cuckoo25", chunksData);
    });

    // check the bucket size
    Map<String, Long> info = cl.cfInfo("cuckoo25");

    // check the retrieved filter properties
    assertEquals(1L, info.get("Number of items inserted").longValue());
    assertEquals(50L, info.get("Bucket size").longValue());
    assertEquals(0L, info.get("Number of items deleted").longValue());

    // check for existing items
    assertTrue(cl.cfExists("cuckoo25", "b"));
  }
  
  @Test
  public void testScanDumpWithStream() {
    cl.cfCreate("cuckoo26", 100, 50);
    cl.cfAdd("cuckoo26", "c");
    
    List<IteratorDataPair> chunks = cl.cfScanDumpStream("cuckoo26").collect(Collectors.toList());
    
    cl.delete("cuckoo26");

    // Load it back
    chunks.forEach(chunksData -> {
      cl.cfLoadChunk("cuckoo26", chunksData);
    });

    // check the bucket size
    Map<String, Long> info = cl.cfInfo("cuckoo26");

    // check the retrieved filter properties
    assertEquals(1L, info.get("Number of items inserted").longValue());
    assertEquals(50L, info.get("Bucket size").longValue());
    assertEquals(0L, info.get("Number of items deleted").longValue());

    // check for existing items
    assertTrue(cl.cfExists("cuckoo26", "c"));
  }

}
