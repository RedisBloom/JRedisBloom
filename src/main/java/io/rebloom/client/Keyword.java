package io.rebloom.client;

import redis.clients.jedis.util.SafeEncoder;

import java.util.Locale;

public enum Keyword {
    CAPACITY,
    ERROR,
    NOCREATE,
    ITEMS;

    public final byte[] raw;

    Keyword() {
        raw = SafeEncoder.encode(this.name().toLowerCase(Locale.ENGLISH));
    }
}
