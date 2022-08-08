package io.rebloom.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.rebloom.client.td.TDigestValueWeight;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import org.junit.Test;
import redis.clients.jedis.exceptions.JedisDataException;

public class TDigestTest extends TestBase {

  private static final Random random = new Random();

  private void assertMergedUnmergedNodes(String key, int mergedNodes, int unmergedNodes) {
    assertEquals(Long.valueOf(mergedNodes), cl.tdigestInfo(key).get("Merged nodes"));
    assertEquals(Long.valueOf(unmergedNodes), cl.tdigestInfo(key).get("Unmerged nodes"));
  }

  @Test
  public void createAndInfo() {
    for (int i = 100; i < 1000; i += 100) {
      String key = "td-" + i;
      cl.tdigestCreate(key, i);

      Map<String, Object> info = cl.tdigestInfo(key);
      assertEquals(Long.valueOf(i), info.get("Compression"));
    }
  }

  @Test
  public void infoNone() {
    try {
      cl.tdigestInfo("none");
      fail("key does not exist");
    } catch (JedisDataException jde) {
      assertEquals("ERR T-Digest: key does not exist", jde.getMessage());
    }
  }

  @Test
  public void reset() {
    cl.tdigestCreate("reset", 100);
    assertMergedUnmergedNodes("reset", 0, 0);

    // on empty
    cl.tdigestReset("reset");
    assertMergedUnmergedNodes("reset", 0, 0);

    cl.tdigestAdd("reset", randomAddParam(), randomAddParam(), randomAddParam());
    assertMergedUnmergedNodes("reset", 0, 3);

    cl.tdigestReset("reset");
    assertMergedUnmergedNodes("reset", 0, 0);
  }

  @Test
  public void add() {
    cl.tdigestCreate("tdadd", 100);

    cl.tdigestAdd("tdadd", randomAddParam());
    assertMergedUnmergedNodes("tdadd", 0, 1);

    cl.tdigestAdd("tdadd", randomAddParam(), randomAddParam(), randomAddParam(), randomAddParam());
    assertMergedUnmergedNodes("tdadd", 0, 5);
  }

  @Test
  public void addNone() {
    cl.tdigestCreate("tdadd", 100);

    try {
      cl.tdigestAdd("tdadd");
      fail("wrong number of arguments");
    } catch (JedisDataException jde) {
      assertEquals("ERR wrong number of arguments for 'TDIGEST.ADD' command", jde.getMessage());
    }
  }

  @Test
  public void merge() {
    cl.tdigestCreate("td2", 100);
    cl.tdigestCreate("td4m", 100);

    cl.tdigestMerge("td2", "td4m");
    assertMergedUnmergedNodes("td2", 0, 0);

    cl.tdigestAdd("td2", definedAddParam(1, 1), definedAddParam(1, 1), definedAddParam(1, 1));
    cl.tdigestAdd("td4m", definedAddParam(1, 100), definedAddParam(1, 100));

    cl.tdigestMerge("td2", "td4m");
    assertMergedUnmergedNodes("td2", 3, 2);
  }

  @Test
  public void mergeFromNone() {
    cl.tdigestCreate("td2", 100);
    try {
      cl.tdigestMerge("td2", "td4m");
      fail("key does not exist");
    } catch (JedisDataException jde) {
      assertEquals("ERR T-Digest: key does not exist", jde.getMessage());
    }
  }

  @Test
  public void mergeToNone() {
    cl.tdigestCreate("td4m", 100);
    try {
      cl.tdigestMerge("td2", "td4m");
      fail("key does not exist");
    } catch (JedisDataException jde) {
      assertEquals("ERR T-Digest: key does not exist", jde.getMessage());
    }
  }

  @Test
  public void cdf() {
    try {
      cl.tdigestCDF("tdcdf", 50);
      fail("key does not exist");
    } catch (JedisDataException jde) {
      assertEquals("ERR T-Digest: key does not exist", jde.getMessage());
    }

    cl.tdigestCreate("tdcdf", 100);
    assertEquals(Double.NaN, cl.tdigestCDF("tdcdf", 50), 0d);

    cl.tdigestAdd("tdcdf", definedAddParam(1, 1), definedAddParam(1, 1), definedAddParam(1, 1));
    cl.tdigestAdd("tdcdf", definedAddParam(100, 1), definedAddParam(100, 1));
    assertEquals(0.6, cl.tdigestCDF("tdcdf", 50), 0.01);
  }

  @Test
  public void quantile() {
    try {
      cl.tdigestQuantile("tdqnt", 0.5);
      fail("key does not exist");
    } catch (JedisDataException jde) {
      assertEquals("ERR T-Digest: key does not exist", jde.getMessage());
    }

    cl.tdigestCreate("tdqnt", 100);
    assertEquals(Collections.singletonMap(0.5, Double.NaN), cl.tdigestQuantile("tdqnt", 0.5));

    cl.tdigestAdd("tdqnt", definedAddParam(1, 1), definedAddParam(1, 1), definedAddParam(1, 1));
    cl.tdigestAdd("tdqnt", definedAddParam(100, 1), definedAddParam(100, 1));
    assertEquals(Collections.singletonMap(0.5, 1.0), cl.tdigestQuantile("tdqnt", 0.5));
  }

  @Test
  public void minAndMax() {
    final String key = "tdmnmx";
    try {
      cl.tdigestMin(key);
      fail("key does not exist");
    } catch (JedisDataException jde) {
      assertEquals("ERR T-Digest: key does not exist", jde.getMessage());
    }
    try {
      cl.tdigestMax(key);
      fail("key does not exist");
    } catch (JedisDataException jde) {
      assertEquals("ERR T-Digest: key does not exist", jde.getMessage());
    }

    cl.tdigestCreate(key, 100);
//    assertEquals(Double.MAX_VALUE, cl.tdigestMin(key), 0d);
//    assertEquals(Double.MIN_NORMAL, cl.tdigestMax(key), 0d);
    assertTrue((Object) cl.tdigestMin(key) instanceof Double);
    assertTrue((Object) cl.tdigestMax(key) instanceof Double);

    cl.tdigestAdd(key, definedAddParam(2, 1));
    cl.tdigestAdd(key, definedAddParam(5, 1));
    assertEquals(2d, cl.tdigestMin(key), 0.01);
    assertEquals(5d, cl.tdigestMax(key), 0.01);
  }

  private static TDigestValueWeight randomAddParam() {
    return new TDigestValueWeight(random.nextDouble() * 10000, random.nextDouble() * 500 + 1);
  }

  private static TDigestValueWeight definedAddParam(double value, double weight) {
    return new TDigestValueWeight(value, weight);
  }
}
