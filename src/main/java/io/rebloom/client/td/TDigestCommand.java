package io.rebloom.client.td;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum TDigestCommand implements ProtocolCommand {

  CREATE, INFO, ADD, RESET, MERGE,
  CDF;

  private final byte[] raw;

  private TDigestCommand() {
    raw = SafeEncoder.encode("TDIGEST." + name());
  }

  @Override
  public byte[] getRaw() {
    return raw;
  }
}
