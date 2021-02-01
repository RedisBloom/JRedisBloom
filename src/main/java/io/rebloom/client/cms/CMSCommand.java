package io.rebloom.client.cms;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum CMSCommand implements ProtocolCommand {
  INITBYDIM("CMS.INITBYDIM"), //
  INITBYPROB("CMS.INITBYPROB"), //
  INCRBY("CMS.INCRBY"), //
  QUERY("CMS.QUERY"), //
  MERGE("CMS.MERGE"), //
  INFO("CMS.INFO");

  private final byte[] raw;

  CMSCommand(String alt) {
    raw = SafeEncoder.encode(alt);
  }

  public byte[] getRaw() {
    return raw;
  }
}
