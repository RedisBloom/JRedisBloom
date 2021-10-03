package io.rebloom.client;

import static io.rebloom.client.Keywords.EXPANSION;
import static io.rebloom.client.Keywords.NONSCALING;
import static redis.clients.jedis.Protocol.toByteArray;

import java.util.ArrayList;
import java.util.List;

public class ReserveParams {

  private int expnasion = -1;
  private boolean nonScaling = false;

  public ReserveParams() {
  }

  public static ReserveParams reserveParams() {
    return new ReserveParams();
  }

  public ReserveParams expansion(int expansion) {
    this.expnasion = expansion;
    return this;
  }

  public ReserveParams nonScaling() {
    this.nonScaling = true;
    return this;
  }

  public List<byte[]> getParams() {
    List<byte[]> args = new ArrayList<>();
    if (expnasion >= 0) {
      args.add(EXPANSION.getRaw());
      args.add(toByteArray(expnasion));
    }
    if (nonScaling) {
      args.add(NONSCALING.getRaw());
    }
    return args;
  }
}
