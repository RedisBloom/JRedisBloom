package io.rebloom.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum TopKCommand implements ProtocolCommand {
    RESERVE("TOPK.RESERVE"),
    ADD("TOPK.ADD"),
    INCRBY("TOPK.INCRBY"),
    QUERY("TOPK.QUERY"),
    COUNT("TOPK.COUNT"),
    LIST("TOPK.LIST"),
    INFO("TOPK.INFO");

    private final byte[] raw;

    TopKCommand(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    @Override
    public byte[] getRaw() {
        return raw;
    }
}
