package io.rebloom.client;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.Client;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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


    /**
     * Reserve a bloom filter.
     * @param name The key of the filter
     * @param initCapacity Optimize for this many items
     * @param errorRate The desired rate of false positives
     *
     * Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
     * is called.
     */
    public boolean createFilter(String name, long initCapacity, double errorRate) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
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
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.ADD, name, value);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
    }

    /**
     * Like {@link #add(String, String)}, but allows you to store non-string items
     * @param name Name of the filter
     * @param value Value to add to the filter
     * @return true if the item was not previously in the filter
     */
    public boolean add(String name, byte[] value) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.ADD, name.getBytes(), value);
                return conn.getIntegerReply() != 0;
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
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.EXISTS, name, value);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
    }

    /**
     * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
     * @param name Key of the filter to check
     * @param value Value to check for
     * @return true if the item may exist in the filter, false if the item does not exist in the filter.
     */
    public boolean exists(String name, byte[] value) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
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
            public boolean[] execute(Jedis connection) {
                Connection conn = connection.getClient();
                return sendMultiCommand(conn, Command.MADD, name.getBytes(), values);
            }
        }).run(name);
    }

    public boolean[] addMulti(String name, String... values){
        return (new JedisClusterCommand<boolean[]>(this.connectionHandler, this.maxAttempts) {
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
            public boolean[] execute(Jedis connection) {
                Connection conn = connection.getClient();
                return sendMultiCommand(conn, Command.MEXISTS, name.getBytes(), values);
            }
        }).run(name);
    }

    public boolean[] existsMulti(String name, String... values) {
        return (new JedisClusterCommand<boolean[]>(this.connectionHandler, this.maxAttempts) {
            public boolean[] execute(Jedis connection) {
                Connection conn = connection.getClient();
                return sendMultiCommand(conn, Command.MEXISTS, name, values);
            }
        }).run(name);
    }

    public boolean delete(String name) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                ((Client) conn).del(name);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
    }



    @SafeVarargs
    private final <T> boolean[] sendMultiCommand(Connection conn, Command cmd, T name, T... value) {
        ArrayList<T> arr = new ArrayList<>();
        arr.add(name);
        arr.addAll(Arrays.asList(value));
        List<Long> reps;
        if (name instanceof String) {
            conn.sendCommand(cmd, (String[]) arr.toArray((String[]) value));
            reps = conn.getIntegerMultiBulkReply();
        } else {
            conn.sendCommand(cmd, (byte[][]) arr.toArray((byte[][]) value));
            reps = conn.getIntegerMultiBulkReply();
        }
        boolean[] ret = new boolean[value.length];
        for (int i = 0; i < reps.size(); i++) {
            ret[i] = reps.get(i) != 0;
        }

        return ret;
    }


}
