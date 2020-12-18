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

import io.rebloom.client.cf.CFInsertOptions;
import io.rebloom.client.cf.CFReserveOptions;
import redis.clients.jedis.exceptions.JedisDataException;

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
    cl.cfCreate("cuckoo2", CFReserveOptions.builder()//
        .withCapacity(200) //
        .withBucketSize(10) //
        .build());
    
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
    cl.cfCreate("cuckoo3", CFReserveOptions.builder()//
        .withCapacity(200)  //
        .withBucketSize(10)  //
        .withMaxIterations(20)  //
        .build());
    
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
    cl.cfCreate("cuckoo4", CFReserveOptions.builder()//
        .withCapacity(200)  //
        .withBucketSize(10)  //
        .withExpansion(4)  //
        .withMaxIterations(20) //
        .build());
    
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
        cl.cfInsert("cuckoo9", //
            CFInsertOptions.builder()//
                .withCapacity(1000)//
                .build(), //
            "foo").toArray(new Boolean[0]) //
    );
  }

  @Test
  public void testInsertNoCreateFilterDoesNotExist() {
    Exception ex = assertThrows(JedisDataException.class, () -> {
      cl.cfInsert("cuckoo10", //
          CFInsertOptions.builder() //
              .noCreate() //
              .build(), //
          "foo", "bar");
    });
    assertTrue(ex.getMessage().contains("ERR not found"));
  }

  @Test
  public void testInsertNoCreateFilterExists() {
    cl.cfInsert("cuckoo11", "bar");
    assertArrayEquals( //
        new Boolean[] { true, true }, //
        cl.cfInsert("cuckoo11", //
            CFInsertOptions.builder()//
                .noCreate() //
                .build(), //
            "foo", "bar").toArray(new Boolean[0]) //
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
        cl.cfInsertNx("cuckoo13", //
            CFInsertOptions.builder() //
                .withCapacity(1000) //
                .build(),
            "bar").toArray(new Boolean[0]) //
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
      cl.cfInsertNx("cuckoo15", CFInsertOptions.builder().noCreate().build(), "foo", "bar");
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
    cl.cfCreate("cuckoo24", CFReserveOptions.builder()//
        .withCapacity(100)  //
        .withBucketSize(50)  //
        .build());
    
    cl.cfAdd("cuckoo24", "a");

    long iter = 0;
    List<Map.Entry<Long, byte[]>> chunks = new ArrayList<>();
    while (true) {
      Map.Entry<Long, byte[]> chunksData = cl.cfScanDump("cuckoo24", iter);
      iter = chunksData.getKey();
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
    cl.cfCreate("cuckoo25", CFReserveOptions.builder()//
        .withCapacity(200)  //
        .withBucketSize(50)  //
        .build());
    
    cl.cfAdd("cuckoo25", "b");
    
    List<Map.Entry<Long, byte[]>> chunks = new ArrayList<>();
    Iterator<Map.Entry<Long, byte[]>> i = cl.cfScanDumpIterator("cuckoo25");
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
    cl.cfCreate("cuckoo26", CFReserveOptions.builder()//
        .withCapacity(100)  //
        .withBucketSize(50)  //
        .build());
    
    cl.cfAdd("cuckoo26", "c");
    
    List<Map.Entry<Long, byte[]>> chunks = cl.cfScanDumpStream("cuckoo26").collect(Collectors.toList());
    
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
