package io.rebloom.client.td;

public class TDigestValueWeight {

  private final double value;
  private final long weight;

  public TDigestValueWeight(double value, long weight) {
    this.value = value;
    this.weight = weight;
  }

  public double getValue() {
    return value;
  }

  public long getWeight() {
    return weight;
  }
}
