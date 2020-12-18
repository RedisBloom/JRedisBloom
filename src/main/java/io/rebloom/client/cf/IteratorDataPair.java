package io.rebloom.client.cf;

public class IteratorDataPair {
  private long iteratorValue;
  private byte[] data;

  public IteratorDataPair(Long iteratorValue, byte[] data) {
    this.iteratorValue = iteratorValue;
    this.data = data;
  }

  public long getIteratorValue() {
    return iteratorValue;
  }

  public byte[] getData() {
    return data;
  }
}
