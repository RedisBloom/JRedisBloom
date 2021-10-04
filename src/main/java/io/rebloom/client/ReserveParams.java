package io.rebloom.client;

import static io.rebloom.client.Keywords.EXPANSION;
import static io.rebloom.client.Keywords.NONSCALING;
import static redis.clients.jedis.Protocol.toByteArray;

import java.util.ArrayList;
import java.util.List;

/**
 * To be used by both BF.RESERVE and BR.INSERT commands.
 * <p>
 * Supported arguments:
 * <ul><li>EXPANSION</li><li>NONSCALING</li></ul>
 */
public class ReserveParams {

  private int expansion = 0;
  private boolean nonScaling = false;

  public ReserveParams() {
  }

  /**
   * @return ReserveParams
   * @see ReserveParams
   */
  public static ReserveParams reserveParams() {
    return new ReserveParams();
  }

  public ReserveParams expansion(int expansion) {
    this.expansion = expansion;
    return this;
  }

  public ReserveParams nonScaling() {
    this.nonScaling = true;
    return this;
  }

  public List<byte[]> getParams() {
    List<byte[]> args = new ArrayList<>();
    if (expansion > 0) {
      args.add(EXPANSION.getRaw());
      args.add(toByteArray(expansion));
    }
    if (nonScaling) {
      args.add(NONSCALING.getRaw());
    }
    return args;
  }
}
