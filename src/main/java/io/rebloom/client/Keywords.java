package io.rebloom.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Keywords implements ProtocolCommand {
    CAPACITY,
    ERROR,
    NOCREATE,
    EXPANSION,
    NONSCALING,
    ITEMS;

    private final byte[] raw;

    Keywords() {
        raw = SafeEncoder.encode(name());
    }

    @Override
    public byte[] getRaw() {
      return raw;
    }
}
