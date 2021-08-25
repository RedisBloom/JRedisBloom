package io.rebloom.client;

import org.junit.Before;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.Pool;

public class TestBase {

  private final Pool<Jedis> pool = new JedisPool();
  protected final Client cl = new Client(pool);

  @Before
  public void clearDB() {
    try (Jedis jedis = pool.getResource()) {
      jedis.flushDB();
    }
  }
}
