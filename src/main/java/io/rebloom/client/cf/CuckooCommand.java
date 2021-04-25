package io.rebloom.client.cf;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum CuckooCommand implements ProtocolCommand {
  RESERVE("CF.RESERVE"), //
  ADD("CF.ADD"), //
  ADDNX("CF.ADDNX"), //
  INSERT("CF.INSERT"), //
  INSERTNX("CF.INSERTNX"), //
  EXIST("CF.EXISTS"), //
  DEL("CF.DEL"), //
  COUNT("CF.COUNT"), //
  SCANDUMP("CF.SCANDUMP"), //
  LOADCHUNK("CF.LOADCHUNK"), //
  INFO("CF.INFO");

  private final byte[] raw;

  CuckooCommand(String alt) {
    raw = SafeEncoder.encode(alt);
  }

  @Override
  public byte[] getRaw() {
    return raw;
  }
}
