package io.rebloom.client;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.rebloom.client.cf.Cuckoo;
import io.rebloom.client.cf.CuckooCommand;
import io.rebloom.client.cf.IteratorDataPair;

/**
 * Client is the main ReBloom client class, wrapping connection management and all ReBloom commands
 */
public class Client implements Cuckoo, Closeable {
  private final Pool<Jedis> pool;
  
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
      String rep = sendCommand(conn, Command.RESERVE, SafeEncoder.encode(name), Protocol.toByteArray(errorRate), Protocol.toByteArray(initCapacity)).getStatusCodeReply();
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
    return add(name, SafeEncoder.encode(value));
  }

  /**
   * Like {@link #add(String, String)}, but allows you to store non-string items
   * @param name Name of the filter
   * @param value Value to add to the filter
   * @return true if the item was not previously in the filter
   */
  public boolean add(String name, byte[] value) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, Command.ADD, SafeEncoder.encode(name), value).getIntegerReply() != 0;
    }
  }

  /**
   * add one or more items to the bloom filter, by default creating it if it does not yet exist
   *
   * @param name The name of the filter
   * @param options {@link io.rebloom.client.InsertOptions}
   * @param items items to add to the filter
   * @return
   */
  public boolean[] insert(String name, InsertOptions options, String... items) {
    final List<byte[]> args = new ArrayList<>();
    args.addAll(options.getOptions());
    args.add(Keywords.ITEMS.getRaw());
    for (String item : items) {
      args.add(SafeEncoder.encode(item));
    }
    return sendMultiCommand(Command.INSERT, SafeEncoder.encode(name), args.toArray(new byte[args.size()][]));
  }

  @SafeVarargs
  private final boolean[] sendMultiCommand(Command cmd, byte[] name, byte[]... values) {
    byte[][] args = new byte[values.length + 1][];
    args[0] = name;
    System.arraycopy(values, 0, args, 1, values.length);
    List<Long> reps;
    try (Jedis conn = _conn()) {
      reps = sendCommand(conn, cmd, args).getIntegerMultiBulkReply();
    }
    boolean[] ret = new boolean[values.length];    
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
   * @see #add(String, byte[])
   */
  public boolean[] addMulti(String name, byte[] ...values) {
    return sendMultiCommand(Command.MADD, SafeEncoder.encode(name), values);
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
  public boolean[] addMulti(String name, String ...values) {
    return addMulti(name, SafeEncoder.encodeMany(values));
  }

  /**
   * Check if an item exists in the filter
   * @param name Name (key) of the filter
   * @param value Value to check for
   * @return true if the item may exist in the filter, false if the item does not exist in the filter
   */
  public boolean exists(String name, String value) {
    return exists(name, SafeEncoder.encode(value));
  }

  /**
   * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
   * @param name Key of the filter to check
   * @param value Value to check for
   * @return true if the item may exist in the filter, false if the item does not exist in the filter.
   */
  public boolean exists(String name, byte[] value) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, Command.EXISTS, SafeEncoder.encode(name), value).getIntegerReply() != 0;
    }
  }

  /**
   * Check if one or more items exist in the filter
   * @param name Name of the filter to check
   * @param values values to check for
   * @return An array of booleans. A true value means the corresponding value may exist, false means it does not exist
   */
  public boolean[] existsMulti(String name, byte[] ...values) {
    return sendMultiCommand(Command.MEXISTS, SafeEncoder.encode(name), values);
  }

  public boolean[] existsMulti(String name, String ...values) {
    return sendMultiCommand(Command.MEXISTS, SafeEncoder.encode(name), SafeEncoder.encodeMany(values));
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

  /**
   * Get information about the filter
   * @param name
   * @return Return information
   */
  public Map<String, Object> info(String name) {
    try (Jedis conn = _conn()) {
      List<Object> values = sendCommand(conn, Command.INFO, SafeEncoder.encode(name)).getObjectMultiBulkReply();

      Map<String, Object> infoMap = new HashMap<>(values.size() / 2);
      for (int i = 0; i < values.size(); i += 2) {
        Object val = values.get(i + 1);
        if (val instanceof byte[]) {
          val = SafeEncoder.encode((byte[]) val);
        }
        infoMap.put(SafeEncoder.encode((byte[]) values.get(i)), val);
      }
      return infoMap;
    }
  }

  /**
   * TOPK.RESERVE key topk width depth decay
   *
   * Reserve a topk filter.
   * @param key The key of the filter
   * @param topk
   * @param width
   * @param depth
   * @param decay
   *
   * Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
   * is called.
   */
  public void topkCreateFilter(String key, long topk, long width, long depth, double decay) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, TopKCommand.RESERVE,  SafeEncoder.encode(key), Protocol.toByteArray(topk),
          Protocol.toByteArray(width), Protocol.toByteArray(depth),Protocol.toByteArray(decay))
          .getStatusCodeReply();

      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

 /**
  * TOPK.ADD key item [item ...]
  *
  * Adds an item to the filter
  * @param key The key of the filter
  * @param items The items to add to the filter
  * @return list of items dropped from the list.
  */
 public List<String> topkAdd(String key, String ...items) {
   try (Jedis conn = _conn()) {
     return sendCommand(conn, key, TopKCommand.ADD, items).getMultiBulkReply();
   }
 }

 /**
  * TOPK.INCRBY key item increment [item increment ...]
  *
  * Adds an item to the filter
  * @param key The key of the filter
  * @param item The item to increment
  * @return item dropped from the list.
  */
 public String topkIncrBy(String key, String item, long increment) {
   try (Jedis conn = _conn()) {
     return sendCommand(conn, TopKCommand.INCRBY, SafeEncoder.encode(key), SafeEncoder.encode(item), Protocol.toByteArray(increment))
         .getMultiBulkReply().get(0);
   }
 }

 /**
  * TOPK.QUERY key item [item ...]
  *
  * Checks whether an item is one of Top-K items.
  *
  * @param key The key of the filter
  * @param items The items to check in the list
  * @return list of indicator for each item requested
  */
 public List<Boolean> topkQuery(String key, String ...items) {
   try (Jedis conn = _conn()) {
     return sendCommand(conn, key, TopKCommand.QUERY, items)
         .getIntegerMultiBulkReply()
         .stream().map(s -> s!=0)
         .collect(Collectors.toList());
   }
 }

 /**
  * TOPK.COUNT key item [item ...]
  *
  * Returns count for an item.
  *
  * @param key The key of the filter
  * @param items The items to check in the list
  * @return list of counters per item.
  */
 public List<Long> topkCount(String key, String ...items) {
   try (Jedis conn = _conn()) {
     return sendCommand(conn, key, TopKCommand.COUNT, items)
         .getIntegerMultiBulkReply();
   }
 }

 /**
  * TOPK.LIST key
  *
  * Return full list of items in Top K list.
  *
  * @param key The key of the filter
  * @return list of items in the list.
  */
 public List<String> topkList(String key) {
   try (Jedis conn = _conn()) {
     return sendCommand(conn, TopKCommand.LIST, SafeEncoder.encode(key))
         .getMultiBulkReply();
   }
 }

  @Override
  public void close(){
    this.pool.close();
  }

  Jedis _conn() {
    return pool.getResource();
  }

  private Connection sendCommand(Jedis conn, String key, ProtocolCommand command, String ...args) {
    byte[][] fullArgs = new byte[args.length + 1][];
    fullArgs[0] = SafeEncoder.encode(key);
    System.arraycopy( SafeEncoder.encodeMany(args), 0, fullArgs, 1, args.length);
    return sendCommand(conn, command, fullArgs);
  }

  private Connection sendCommand(Jedis conn, ProtocolCommand command, byte[]... args) {
    Connection client = conn.getClient();
    client.sendCommand(command, args);
    return client;
  }

  //
  // Cuckoo Filter Implementation
  //

  @Override
  public void cfCreate(String key, long capacity) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, CuckooCommand.RESERVE, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(capacity)).getStatusCodeReply();

      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

  @Override
  public void cfCreate(String key, long capacity, long bucketSize) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, CuckooCommand.RESERVE, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(capacity), //
          SafeEncoder.encode("BUCKETSIZE"), //
          Protocol.toByteArray(bucketSize) //
      ).getStatusCodeReply();

      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

  @Override
  public void cfCreate(String key, long capacity, long bucketSize, long maxIterations) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, CuckooCommand.RESERVE, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(capacity), //
          SafeEncoder.encode("BUCKETSIZE"), //
          Protocol.toByteArray(bucketSize), //
          SafeEncoder.encode("MAXITERATIONS"), //
          Protocol.toByteArray(maxIterations) //
      ).getStatusCodeReply();

      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

  @Override
  public void cfCreate(String key, long capacity, long bucketSize, long maxIterations, long expansion) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, CuckooCommand.RESERVE, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(capacity), //
          SafeEncoder.encode("BUCKETSIZE"), //
          Protocol.toByteArray(bucketSize), //
          SafeEncoder.encode("MAXITERATIONS"), //
          Protocol.toByteArray(maxIterations), //
          SafeEncoder.encode("EXPANSION"), //
          Protocol.toByteArray(expansion)).getStatusCodeReply();

      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

  @Override
  public boolean cfAdd(String key, String item) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, CuckooCommand.ADD, //
          SafeEncoder.encode(key), //
          SafeEncoder.encode(item) //
      ).getIntegerReply() == 1;
    }
  }

  @Override
  public boolean cfAddNx(String key, String item) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, CuckooCommand.ADDNX, //
          SafeEncoder.encode(key), //
          SafeEncoder.encode(item) //
      ).getIntegerReply() == 1;
    }
  }

  @Override
  public List<Boolean> cfInsert(String key, String... items) {
    try (Jedis conn = _conn()) {

      byte[][] fullArgs = new byte[items.length + 2][];
      fullArgs[0] = SafeEncoder.encode(key);
      fullArgs[1] = SafeEncoder.encode("ITEMS");
      System.arraycopy(SafeEncoder.encodeMany(items), 0, fullArgs, 2, items.length);

      return sendCommand(conn, CuckooCommand.INSERT, fullArgs) //
          .getIntegerMultiBulkReply() //
          .stream() //
          .map(s -> s > 0) //
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Boolean> cfInsert(String key, long capacity, String... items) {
    try (Jedis conn = _conn()) {

      byte[][] fullArgs = new byte[items.length + 4][];
      fullArgs[0] = SafeEncoder.encode(key);
      fullArgs[1] = SafeEncoder.encode("CAPACITY");
      fullArgs[2] = Protocol.toByteArray(capacity);
      fullArgs[3] = SafeEncoder.encode("ITEMS");
      System.arraycopy(SafeEncoder.encodeMany(items), 0, fullArgs, 4, items.length);

      return sendCommand(conn, CuckooCommand.INSERT, fullArgs) //
          .getIntegerMultiBulkReply() //
          .stream() //
          .map(s -> s > 0) //
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Boolean> cfInsertNoCreate(String key, String... items) {
    try (Jedis conn = _conn()) {

      byte[][] fullArgs = new byte[items.length + 3][];
      fullArgs[0] = SafeEncoder.encode(key);
      fullArgs[1] = SafeEncoder.encode("NOCREATE");
      fullArgs[2] = SafeEncoder.encode("ITEMS");
      System.arraycopy(SafeEncoder.encodeMany(items), 0, fullArgs, 3, items.length);

      return sendCommand(conn, CuckooCommand.INSERT, fullArgs) //
          .getIntegerMultiBulkReply() //
          .stream() //
          .map(s -> s > 0) //
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Boolean> cfInsertNx(String key, String... items) {
    try (Jedis conn = _conn()) {

      byte[][] fullArgs = new byte[items.length + 2][];
      fullArgs[0] = SafeEncoder.encode(key);
      fullArgs[1] = SafeEncoder.encode("ITEMS");
      System.arraycopy(SafeEncoder.encodeMany(items), 0, fullArgs, 2, items.length);

      return sendCommand(conn, CuckooCommand.INSERTNX, fullArgs) //
          .getIntegerMultiBulkReply() //
          .stream() //
          .map(s -> s > 0) //
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Boolean> cfInsertNx(String key, long capacity, String... items) {
    try (Jedis conn = _conn()) {

      byte[][] fullArgs = new byte[items.length + 4][];
      fullArgs[0] = SafeEncoder.encode(key);
      fullArgs[1] = SafeEncoder.encode("CAPACITY");
      fullArgs[2] = Protocol.toByteArray(capacity);
      fullArgs[3] = SafeEncoder.encode("ITEMS");
      System.arraycopy(SafeEncoder.encodeMany(items), 0, fullArgs, 4, items.length);

      return sendCommand(conn, CuckooCommand.INSERTNX, fullArgs) //
          .getIntegerMultiBulkReply() //
          .stream() //
          .map(s -> s > 0) //
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Boolean> cfInsertNxNoCreate(String key, String... items) {
    try (Jedis conn = _conn()) {

      byte[][] fullArgs = new byte[items.length + 3][];
      fullArgs[0] = SafeEncoder.encode(key);
      fullArgs[1] = SafeEncoder.encode("NOCREATE");
      fullArgs[2] = SafeEncoder.encode("ITEMS");
      System.arraycopy(SafeEncoder.encodeMany(items), 0, fullArgs, 3, items.length);

      return sendCommand(conn, CuckooCommand.INSERT, fullArgs) //
          .getIntegerMultiBulkReply() //
          .stream() //
          .map(s -> s > 0) //
          .collect(Collectors.toList());
    }
  }

  @Override
  public boolean cfExists(String key, String item) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, CuckooCommand.EXIST, //
          SafeEncoder.encode(key), //
          SafeEncoder.encode(item) //
      ).getIntegerReply() == 1;
    }
  }

  @Override
  public boolean cfDel(String key, String item) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, CuckooCommand.DEL, //
          SafeEncoder.encode(key), //
          SafeEncoder.encode(item) //
      ).getIntegerReply() == 1;
    }
  }

  @Override
  public long cfCount(String key, String item) {
    try (Jedis conn = _conn()) {
      return sendCommand(conn, CuckooCommand.COUNT, //
          SafeEncoder.encode(key), //
          SafeEncoder.encode(item) //
      ).getIntegerReply();
    }
  }

  @Override
  public IteratorDataPair cfScanDump(String key, long iteratorValue) {
    try (Jedis conn = _conn()) {
      List<Object> response = sendCommand(conn, CuckooCommand.SCANDUMP, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(iteratorValue) //
      ).getObjectMultiBulkReply();

      return new IteratorDataPair(((Long) response.get(0)).longValue(), (byte[]) response.get(1));
    }
  }

  @Override
  public void cfLoadChunk(String key, IteratorDataPair idp) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, CuckooCommand.LOADCHUNK, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(idp.getIteratorValue()), //
          idp.getData() //
      ).getStatusCodeReply();

      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

  @Override
  public Map<String, Long> cfInfo(String key) {
    try (Jedis conn = _conn()) {
      List<Object> values = sendCommand(conn, CuckooCommand.INFO, SafeEncoder.encode(key)).getObjectMultiBulkReply();

      Map<String, Long> infoMap = new HashMap<String, Long>(values.size() / 2);
      for (int i = 0; i < values.size(); i += 2) {
        Long val = (Long) values.get(i + 1);
        infoMap.put(SafeEncoder.encode((byte[]) values.get(i)), val);
      }
      return infoMap;
    }
  }
  
  
  @Override
  public Iterator<IteratorDataPair> cfScanDumpIterator(String key) {
    return new CfScanDumpIterator(this, key);
  }


  @Override
  public Stream<IteratorDataPair> cfScanDumpStream(String key) {
    return StreamSupport.stream(  //
        Spliterators.spliteratorUnknownSize(  //
            new CfScanDumpIterator(this, key), //
            Spliterator.ORDERED //
        ), false);
  }
  
  /**
   * An Iterator over the elements (IteratorDataPair) obtained for a Cuckoo Filter
   * using CF.SCANDUMP.
   *
   */
  private static class CfScanDumpIterator implements Iterator<IteratorDataPair> {

    private Client client;
    private String key;
    private IteratorDataPair current;
    private boolean firstAccess = true;

    /**
     * Create a CfScanDumpIterator for a given Cuckoo Filter using an instance of
     * the Client
     * 
     * @param client RedisBloom Filter client
     * @param key    Name of the filter
     */
    public CfScanDumpIterator(Client client, String key) {
      this.client = client;
      this.key = key;
    }

    @Override
    public boolean hasNext() {
      if (firstAccess) {
        current = pullNext();
      }
      
      return current != null;
    }

    @Override
    public IteratorDataPair next() {
      IteratorDataPair result = current;
      current = pullNext();
      
      return result;
    }
    
    private IteratorDataPair pullNext() {
      IteratorDataPair idp = client.cfScanDump(key, firstAccess ? 0 : current.getIteratorValue());
      if (firstAccess) {
        firstAccess = false;
      }
      
      return idp.getIteratorValue() != 0 ? idp : null;
    }
  }
  
}
