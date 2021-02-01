package io.rebloom.client;

import java.io.Closeable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.rebloom.client.cf.CFInsertOptions;
import io.rebloom.client.cf.CFReserveOptions;
import io.rebloom.client.cf.Cuckoo;
import io.rebloom.client.cf.CuckooCommand;
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

import io.rebloom.client.cms.CMS;
import io.rebloom.client.cms.CMSCommand;

/**
 * Client is the main ReBloom client class, wrapping connection management and all ReBloom commands
 */
public class Client implements Cuckoo, CMS, Closeable {

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
 
 //
 // Count-Min-Sketch Implementation
 //

 @Override
 public void cmsInitByDim(String key, long width, long depth) {
   try (Jedis conn = _conn()) {
     String rep = sendCommand(conn, CMSCommand.INITBYDIM, //
         SafeEncoder.encode(key), //
         Protocol.toByteArray(width), //
         Protocol.toByteArray(depth)).getStatusCodeReply();

     if (!rep.equals("OK")) {
       throw new JedisException(rep);
     }
   }
 }

 @Override
 public void cmsInitByProb(String key, double error, double probability) {
   try (Jedis conn = _conn()) {
     String rep = sendCommand(conn, CMSCommand.INITBYPROB, //
         SafeEncoder.encode(key), //
         Protocol.toByteArray(error), //
         Protocol.toByteArray(probability)).getStatusCodeReply();

     if (!rep.equals("OK")) {
       throw new JedisException(rep);
     }
   }
 }

 @Override
 public long cmsIncrBy(String key, String item, long increment) {
   try (Jedis conn = _conn()) {
     return sendCommand(conn, CMSCommand.INCRBY, //
         SafeEncoder.encode(key), //
         SafeEncoder.encode(item), //
         Protocol.toByteArray(increment)).getIntegerMultiBulkReply().get(0);
   }
 }

 @Override
 public List<Long> cmsIncrBy(String key, Map<String, Long> itemIncrements) {
   try (Jedis conn = _conn()) {
     List<byte[]> mapFlatten = itemIncrements//
         .entrySet() //
         .stream() //
         .flatMap(e -> Stream.of(//
             SafeEncoder.encode(e.getKey()), //
             Protocol.toByteArray(e.getValue())))//
         .collect(Collectors.toList());

     byte[][] fullArgs = new byte[mapFlatten.size() + 1][];
     fullArgs[0] = SafeEncoder.encode(key);
     System.arraycopy(mapFlatten.toArray(), 0, fullArgs, 1, mapFlatten.size());

     return sendCommand(conn, CMSCommand.INCRBY, fullArgs).getIntegerMultiBulkReply();
   }
 }

 @Override
 public List<Long> cmsQuery(String key, String... items) {
   try (Jedis conn = _conn()) {
     return sendCommand(conn, key, CMSCommand.QUERY, items).getIntegerMultiBulkReply();
   }
 }

 @Override
 public void cmsMerge(String destKey, String... keys) {
   try (Jedis conn = _conn()) {
     byte[][] args = new byte[keys.length + 2][];
     args[0] = SafeEncoder.encode(destKey);
     args[1] = Protocol.toByteArray(keys.length);
     System.arraycopy(SafeEncoder.encodeMany(keys), 0, args, 2, keys.length);

     String rep = sendCommand(conn, CMSCommand.MERGE, args).getStatusCodeReply();

     if (!rep.equals("OK")) {
       throw new JedisException(rep);
     }
   }
 }

 @Override
 public void cmsMerge(String destKey, Map<String, Long> keysAndWeights) {
   try (Jedis conn = _conn()) {

     String[] keys = keysAndWeights.keySet().toArray(new String[0]);
     String[] weights = keysAndWeights.values().stream().map(l -> l.toString()).collect(Collectors.toList())
         .toArray(new String[0]);

     byte[][] args = new byte[(keysAndWeights.size() * 2) + 3][];

     args[0] = SafeEncoder.encode(destKey);
     args[1] = Protocol.toByteArray(keys.length);
     System.arraycopy(SafeEncoder.encodeMany(keys), 0, args, 2, keys.length);
     args[keys.length + 2] = SafeEncoder.encode("WEIGHTS");
     System.arraycopy(SafeEncoder.encodeMany(weights), 0, args, weights.length * 2 + 1, weights.length);

     String rep = sendCommand(conn, CMSCommand.MERGE, args).getStatusCodeReply();

     if (!rep.equals("OK")) {
       throw new JedisException(rep);
     }
   }

 }

 @Override
 public Map<String, Long> cmsInfo(String key) {
   try (Jedis conn = _conn()) {
     List<Object> values = sendCommand(conn, CMSCommand.INFO, SafeEncoder.encode(key)).getObjectMultiBulkReply();

     Map<String, Long> infoMap = new HashMap<>(values.size() / 2);
     for (int i = 0; i < values.size(); i += 2) {
       Long val = (Long) values.get(i + 1);
       infoMap.put(SafeEncoder.encode((byte[]) values.get(i)), val);
     }
     return infoMap;
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

  private Connection sendCommand(Jedis conn, ProtocolCommand command, List<byte[]> args) {
    Connection client = conn.getClient();
    client.sendCommand(command, args.toArray(new byte[args.size()][]));
    return client;
  }

  //
  // Cuckoo Filter Implementation
  //

  @Override
  public void cfCreate(String key, CFReserveOptions options) {
    try (Jedis conn = _conn()) {

      List<byte[]> fullArgs = new ArrayList<byte[]>();
      fullArgs.add(SafeEncoder.encode(key));
      fullArgs.addAll(options.asListOfByteArrays());

      String rep = sendCommand(conn, CuckooCommand.RESERVE, fullArgs).getStatusCodeReply();

      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

  @Override
  public void cfCreate(String key, long capacity) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, CuckooCommand.RESERVE, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(capacity) //
      ).getStatusCodeReply();

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

      List<byte[]> fullArgs = new ArrayList<byte[]>();
      fullArgs.add(SafeEncoder.encode(key));
      fullArgs.add(SafeEncoder.encode("ITEMS"));
      fullArgs.addAll(Arrays.asList(SafeEncoder.encodeMany(items)));

      return sendCommand(conn, CuckooCommand.INSERT, fullArgs) //
          .getIntegerMultiBulkReply() //
          .stream() //
          .map(s -> s > 0) //
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Boolean> cfInsert(String key, CFInsertOptions options, String... items) {
    try (Jedis conn = _conn()) {

      List<byte[]> fullArgs = new ArrayList<byte[]>();
      fullArgs.add(SafeEncoder.encode(key));
      fullArgs.addAll(options.asListOfByteArrays());
      fullArgs.add(SafeEncoder.encode("ITEMS"));
      fullArgs.addAll(Arrays.asList(SafeEncoder.encodeMany(items)));

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

      List<byte[]> fullArgs = new ArrayList<byte[]>();
      fullArgs.add(SafeEncoder.encode(key));
      fullArgs.add(SafeEncoder.encode("ITEMS"));
      fullArgs.addAll(Arrays.asList(SafeEncoder.encodeMany(items)));

      return sendCommand(conn, CuckooCommand.INSERTNX, fullArgs) //
          .getIntegerMultiBulkReply() //
          .stream() //
          .map(s -> s > 0) //
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<Boolean> cfInsertNx(String key, CFInsertOptions options, String... items) {
    try (Jedis conn = _conn()) {

      List<byte[]> fullArgs = new ArrayList<byte[]>();
      fullArgs.add(SafeEncoder.encode(key));
      fullArgs.addAll(options.asListOfByteArrays());
      fullArgs.add(SafeEncoder.encode("ITEMS"));
      fullArgs.addAll(Arrays.asList(SafeEncoder.encodeMany(items)));

      return sendCommand(conn, CuckooCommand.INSERTNX, fullArgs) //
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
  public Map.Entry<Long, byte[]> cfScanDump(String key, long iterator) {
    try (Jedis conn = _conn()) {
      List<Object> response = sendCommand(conn, CuckooCommand.SCANDUMP, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(iterator) //
      ).getObjectMultiBulkReply();

      return new AbstractMap.SimpleImmutableEntry<Long, byte[]>(((Long) response.get(0)).longValue(), (byte[]) response.get(1));
    }
  }

  @Override
  public void cfLoadChunk(String key, Map.Entry<Long, byte[]> idp) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, CuckooCommand.LOADCHUNK, //
          SafeEncoder.encode(key), //
          Protocol.toByteArray(idp.getKey()), //
          idp.getValue() //
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
  public Iterator<Map.Entry<Long, byte[]>> cfScanDumpIterator(String key) {
    return new CfScanDumpIterator(this, key);
  }

  @Override
  public Stream<Map.Entry<Long, byte[]>> cfScanDumpStream(String key) {
    return StreamSupport.stream( //
        Spliterators.spliteratorUnknownSize( //
            new CfScanDumpIterator(this, key), //
            Spliterator.ORDERED //
        ), false);
  }

  /**
   * An Iterator over the elements (Map.Entry containing the Iterator and Data)
   * obtained for a Cuckoo Filter using CF.SCANDUMP.
   *
   */
  private static class CfScanDumpIterator implements Iterator<Map.Entry<Long, byte[]>> {

    private final Client client;
    private final String key;
    private Long current;

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
      return scan(current) != null;
    }

    @Override
    public Map.Entry<Long, byte[]> next() {
      Map.Entry<Long, byte[]> dump = scan(current);
      current = dump == null ? 0L : dump.getKey();
      return dump;
    }

    private Map.Entry<Long, byte[]> scan(Long iter) {
      Map.Entry<Long, byte[]> dump = client.cfScanDump(key, iter == null ? 0 : iter);

      return dump.getKey() != 0L ? dump : null;
    }
  }

}
