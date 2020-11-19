package io.rebloom.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Command implements ProtocolCommand {
    RESERVE("BF.RESERVE"),
    ADD("BF.ADD"),
    MADD("BF.MADD"),
    EXISTS("BF.EXISTS"),
    MEXISTS("BF.MEXISTS"),
    INSERT("BF.INSERT"),
    INFO("BF.INFO");

    private final byte[] raw;

    Command(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    public byte[] getRaw() {
        return raw;
    }
}
