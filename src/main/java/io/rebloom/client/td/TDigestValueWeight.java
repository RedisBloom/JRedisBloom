package io.rebloom.client.td;

public class TDigestValueWeight {

  private final double value;
  private final double weight;

  public TDigestValueWeight(double value, double weight) {
    this.value = value;
    this.weight = weight;
  }

  public double getValue() {
    return value;
  }

  public double getWeight() {
    return weight;
  }
}
