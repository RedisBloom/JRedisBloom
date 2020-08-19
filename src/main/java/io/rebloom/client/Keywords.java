package io.rebloom.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Locale;

public enum Keywords implements ProtocolCommand {
    CAPACITY,
    ERROR,
    NOCREATE,
    ITEMS;

    private final byte[] raw;

    Keywords() {
        raw = SafeEncoder.encode(this.name().toLowerCase(Locale.ENGLISH));
    }

    @Override
    public byte[] getRaw() {
      return raw;
    }
}
