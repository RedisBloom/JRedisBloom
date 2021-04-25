package io.rebloom.client;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.Client;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.HashMap;

/**
 * @author TommyYang on 2018/12/17
 */
public class ClusterClient extends JedisCluster {

    public ClusterClient(HostAndPort node) {
        super(node);
    }

    public ClusterClient(HostAndPort node, int timeout) {
        super(node, timeout);
    }

    public ClusterClient(HostAndPort node, int timeout, int maxAttempts) {
        super(node, timeout, maxAttempts);
    }

    public ClusterClient(HostAndPort node, GenericObjectPoolConfig poolConfig) {
        super(node, poolConfig);
    }

    public ClusterClient(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, poolConfig);
    }

    public ClusterClient(HostAndPort node, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, maxAttempts, poolConfig);
    }

    public ClusterClient(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public ClusterClient(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public ClusterClient(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> nodes) {
        super(nodes);
    }

    public ClusterClient(Set<HostAndPort> nodes, int timeout) {
        super(nodes, timeout);
    }

    public ClusterClient(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        super(nodes, timeout, maxAttempts);
    }

    public ClusterClient(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
        super(nodes, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
        super(nodes, timeout, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }


    private void sendCommand(Connection conn, String key, ProtocolCommand command, String ...args) {
        String[] fullArgs = new String[args.length + 1];
        fullArgs[0] = key;
        System.arraycopy(args, 0, fullArgs, 1, args.length);
        conn.sendCommand(command, fullArgs);
    }

    /**
     * Reserve a bloom filter.
     * @param name The key of the filter
     * @param initCapacity Optimize for this many items
     * @param errorRate The desired rate of false positives
     *
     * @return true if the filter create success, false if the filter create error.
     *
     * Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
     * is called.
     */
    public boolean createFilter(String name, long initCapacity, double errorRate) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.RESERVE, name, errorRate + "", initCapacity + "");
                return conn.getStatusCodeReply().equals("OK");
            }
        }).run(name);
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
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.ADD, name.getBytes(), value);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
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
        return (new JedisClusterCommand<boolean[]>(this.connectionHandler, this.maxAttempts) {
            @Override
            public boolean[] execute(Jedis connection) {
                Connection conn = connection.getClient();
                final List<byte[]> args = new ArrayList<>();
                args.addAll(options.getOptions());
                args.add(Keywords.ITEMS.getRaw());
                for (String item : items) {
                    args.add(SafeEncoder.encode(item));
                }
                return sendMultiCommand(conn, Command.INSERT, name.getBytes(), args.toArray(new byte[args.size()][]));
            }
        }).run(name);
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
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.EXISTS, name.getBytes(), value);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
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
    public boolean[] addMulti(String name, byte[]... values){
        return (new JedisClusterCommand<boolean[]>(this.connectionHandler, this.maxAttempts) {
            @Override
            public boolean[] execute(Jedis connection) {
                Connection conn = connection.getClient();
                return sendMultiCommand(conn, Command.MADD, name.getBytes(), values);
            }
        }).run(name);
    }

    public boolean[] addMulti(String name, String... values){
        return (new JedisClusterCommand<boolean[]>(this.connectionHandler, this.maxAttempts) {
            @Override
            public boolean[] execute(Jedis connection) {
                Connection conn = connection.getClient();
                return sendMultiCommand(conn, Command.MADD, name, values);
            }
        }).run(name);
    }

    /**
     * Check if one or more items exist in the filter
     * @param name Name of the filter to check
     * @param values values to check for
     * @return An array of booleans. A true value means the corresponding value may exist, false means it does not exist
     */
    public boolean[] existsMulti(String name, byte[]... values) {
        return (new JedisClusterCommand<boolean[]>(this.connectionHandler, this.maxAttempts) {
            @Override
            public boolean[] execute(Jedis connection) {
                Connection conn = connection.getClient();
                return sendMultiCommand(conn, Command.MEXISTS, name.getBytes(), values);
            }
        }).run(name);
    }

    public boolean[] existsMulti(String name, String... values) {
        return (new JedisClusterCommand<boolean[]>(this.connectionHandler, this.maxAttempts) {
            @Override
            public boolean[] execute(Jedis connection) {
                Connection conn = connection.getClient();
                return sendMultiCommand(conn, Command.MEXISTS, name, values);
            }
        }).run(name);
    }

    /**
     * Remove the filter
     * @param name
     * @return true if delete the filter, false is not delete the filter
     */
    public boolean delete(String name) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                ((Client) conn).del(name);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
    }

    /**
     * Get information about the filter
     * @param name
     * @return Return information
     */
    public Map<String, Object> info(String name) {
        return (new JedisClusterCommand<Map<String, Object>>(this.connectionHandler, this.maxAttempts) {
            @Override
            public Map<String, Object> execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.INFO, name.getBytes());
                List<Object> values = conn.getObjectMultiBulkReply();

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
        }).run(name);
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
        (new JedisClusterCommand<Void>(this.connectionHandler, this.maxAttempts){
            @Override
            public Void execute(Jedis jedis) {
                Connection conn = jedis.getClient();
                conn.sendCommand(TopKCommand.RESERVE, SafeEncoder.encode(key), Protocol.toByteArray(topk),
                        Protocol.toByteArray(width), Protocol.toByteArray(depth),Protocol.toByteArray(decay));
                String resp = conn.getStatusCodeReply();
                if (!resp.equals("OK")){
                    throw new JedisException(resp);
                }
                return null;
            }
        }).run(key);
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
        return (new JedisClusterCommand<List<String>>(this.connectionHandler, this.maxAttempts){
            @Override
            public List<String> execute(Jedis jedis) {
                Connection conn = jedis.getClient();
                sendCommand(conn, key, TopKCommand.ADD, items);
                return conn.getMultiBulkReply();
            }
        }).run(key);
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
        return (new JedisClusterCommand<String>(this.connectionHandler, this.maxAttempts){
            @Override
            public String execute(Jedis jedis) {
                Connection conn = jedis.getClient();
                conn.sendCommand(TopKCommand.INCRBY, SafeEncoder.encode(key), SafeEncoder.encode(item), Protocol.toByteArray(increment));
                return conn.getMultiBulkReply().get(0);
            }
        }).run(key);
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
        return (new JedisClusterCommand<List<Boolean>>(this.connectionHandler, this.maxAttempts){
            @Override
            public List<Boolean> execute(Jedis jedis) {
                Connection conn = jedis.getClient();
                sendCommand(conn, key, TopKCommand.QUERY, items);
                return conn.getIntegerMultiBulkReply()
                        .stream()
                        .map(s -> s != 0)
                        .collect(Collectors.toList());
            }
        }).run(key);
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
        return (new JedisClusterCommand<List<Long>>(this.connectionHandler, this.maxAttempts){
            @Override
            public List<Long> execute(Jedis jedis) {
                Connection conn = jedis.getClient();
                sendCommand(conn, key, TopKCommand.COUNT, items);
                return conn.getIntegerMultiBulkReply();
            }
        }).run(key);
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
        return (new JedisClusterCommand<List<String>>(this.connectionHandler, this.maxAttempts){
            @Override
            public List<String> execute(Jedis jedis) {
                Connection conn = jedis.getClient();
                conn.sendCommand(TopKCommand.LIST, key);
                return conn.getMultiBulkReply();
            }
        }).run(key);
    }

    @SafeVarargs
    private final <T> boolean[] sendMultiCommand(Connection conn, Command cmd, T name, T... value) {
        ArrayList<T> arr = new ArrayList<>();
        arr.add(name);
        arr.addAll(Arrays.asList(value));
        List<Long> reps;
        if (name instanceof String) {
            conn.sendCommand(cmd, arr.toArray((String[]) value));
            reps = conn.getIntegerMultiBulkReply();
        } else {
            conn.sendCommand(cmd, arr.toArray((byte[][]) value));
            reps = conn.getIntegerMultiBulkReply();
        }
        boolean[] ret = new boolean[value.length];
        for (int i = 0; i < reps.size(); i++) {
            ret[i] = reps.get(i) != 0;
        }

        return ret;
    }

}
