package io.rebloom.client;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;

import java.io.Closeable;
import java.util.*;

/**
 * Client is the main ReBloom client class, wrapping connection management and all ReBloom commands
 */
public class Client implements Closeable {
  private final Pool<Jedis> pool;
  
  Jedis _conn() {
    return pool.getResource();
  }

  private Connection sendCommand(Jedis conn, Command command, String ...args) {
    Connection client = conn.getClient();
    client.sendCommand(command, args);
    return client;
  }

  private Connection sendCommand(Jedis conn, Command command, byte[]... args) {
    Connection client = conn.getClient();
    client.sendCommand(command, args);
    return client;
  }

  /**
   * Create a new client to ReBloom
   * @param pool Jedis connection pool to be used 
   */
  public Client(Pool<Jedis> pool){
    this.pool = pool;
  }
 
  
  /**
   * Create a new client to ReBloom
   * @param host the redis host
   * @param port the redis port
   * @param timeout connection timeout
   * @param poolSize the poolSize of JedisPool
   */
  public Client(String host, int port, int timeout, int poolSize) {
    JedisPoolConfig conf = new JedisPoolConfig();
    conf.setMaxTotal(poolSize);
    conf.setTestOnBorrow(false);
    conf.setTestOnReturn(false);
    conf.setTestOnCreate(false);
    conf.setTestWhileIdle(false);
    conf.setMinEvictableIdleTimeMillis(60000);
    conf.setTimeBetweenEvictionRunsMillis(30000);
    conf.setNumTestsPerEvictionRun(-1);
    conf.setFairness(true);

    pool = new JedisPool(conf, host, port, timeout);
  }

  /**
   * Create a new client to ReBloom
   * @param host the redis host
   * @param port the redis port
   */
  public Client(String host, int port) {
    this(host, port, 500, 100);
  }

  /**
   * Reserve a bloom filter.
   * @param name The key of the filter
   * @param initCapacity Optimize for this many items
   * @param errorRate The desired rate of false positives
   *
   * Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
   * is called.
   */
  public void createFilter(String name, long initCapacity, double errorRate) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, Command.RESERVE, name, errorRate + "", initCapacity + "").getStatusCodeReply();

      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

  /**
   * Adds an item to the filter
   * @param name The name of the filter
   * @param value The value to add to the filter
   * @return true if the item was not previously in the filter.
   */
  public boolean add(String name, String value) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, Command.ADD, name, value).getIntegerReply() != 0;
    }
  }

  /**
   * Like {@link #add(String, String)}, but allows you to store non-string items
   * @param name Name of the filter
   * @param value Value to add to the filter
   * @return true if the item was not previously in the filter
   */
  public boolean add(String name, byte[] value) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, Command.ADD, name.getBytes(), value).getIntegerReply() != 0;
    }
  }

  @SafeVarargs
  private final <T> boolean[] sendMultiCommand(Command cmd, T name, T... value) {
    ArrayList<T> arr = new ArrayList<>();
    arr.add(name);
    arr.addAll(Arrays.asList(value));
    List<Long> reps;
    try (Jedis conn = _conn()) {
      if (name instanceof String) {
        reps = sendCommand(conn, cmd, (String[])arr.toArray((String[])value)).getIntegerMultiBulkReply();
      } else {
        reps = sendCommand(conn, cmd, (byte[][])arr.toArray((byte[][])value)).getIntegerMultiBulkReply();
      }
    }
    boolean[] ret = new boolean[value.length];    
    for (int i = 0; i < reps.size(); i++) {
      ret[i] = reps.get(i) != 0;
    }

    return ret;
  }


  /**
   * Add one or more items to a filter
   * @param name Name of the filter
   * @param values values to add to the filter.
   * @return An array of booleans of the same length as the number of values.
   * Each boolean values indicates whether the corresponding element was previously in the
   * filter or not. A true value means the item did not previously exist, whereas a
   * false value means it may have previously existed.
   *
   * @see #add(String, String)
   */
  public boolean[] addMulti(String name, byte[] ...values) {
    return sendMultiCommand(Command.MADD, name.getBytes(), values);
  }

  public boolean[] addMulti(String name, String ...values) {
    return sendMultiCommand(Command.MADD, name, values);
  }

  /**
   * Check if an item exists in the filter
   * @param name Name (key) of the filter
   * @param value Value to check for
   * @return true if the item may exist in the filter, false if the item does not exist in the filter
   */
  public boolean exists(String name, String value) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, Command.EXISTS, name, value).getIntegerReply() != 0;
    }
  }

  /**
   * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
   * @param name Key of the filter to check
   * @param value Value to check for
   * @return true if the item may exist in the filter, false if the item does not exist in the filter.
   */
  public boolean exists(String name, byte[] value) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, Command.EXISTS, name.getBytes(), value).getIntegerReply() != 0;
    }
  }

  /**
   * Check if one or more items exist in the filter
   * @param name Name of the filter to check
   * @param values values to check for
   * @return An array of booleans. A true value means the corresponding value may exist, false means it does not exist
   */
  public boolean[] existsMulti(String name, byte[] ...values) {
    return sendMultiCommand(Command.MEXISTS, name.getBytes(), values);
  }

  public boolean[] existsMulti(String name, String ...values) {
    return sendMultiCommand(Command.MEXISTS, name, values);
  }

  /**
   * Remove the filter
   * @param name
   * @return true if delete the filter, false is not delete the filter
   */
  public boolean delete(String name) {
      try(Jedis conn = _conn()){
          return conn.del(name) != 0;
      }
  }

  @Override
  public void close(){
    this.pool.close();
  }

}
