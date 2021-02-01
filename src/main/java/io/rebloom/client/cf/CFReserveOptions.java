package io.rebloom.client.cf;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class CFReserveOptions {
  private long capacity; // required
  private long bucketSize = -1; // optional
  private long maxIterations = -1; // optional
  private long expansion = -1; // optional

  protected CFReserveOptions() {
  }

  public long getCapacity() {
    return capacity;
  }

  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }

  public long getBucketSize() {
    return bucketSize;
  }

  public void setBucketSize(long bucketSize) {
    this.bucketSize = bucketSize;
  }

  public long getMaxIterations() {
    return maxIterations;
  }

  public void setMaxIterations(long maxIterations) {
    this.maxIterations = maxIterations;
  }

  public long getExpansion() {
    return expansion;
  }

  public void setExpansion(long expansion) {
    this.expansion = expansion;
  }

  public List<byte[]> asListOfByteArrays() {
    List<byte[]> options = new ArrayList<>();

    options.add(Protocol.toByteArray(capacity));

    if (bucketSize != -1) {
      options.add(SafeEncoder.encode("BUCKETSIZE"));
      options.add(Protocol.toByteArray(bucketSize));
    }

    if (maxIterations != -1) {
      options.add(SafeEncoder.encode("MAXITERATIONS"));
      options.add(Protocol.toByteArray(maxIterations));
    }

    if (expansion != -1) {
      options.add(SafeEncoder.encode("EXPANSION"));
      options.add(Protocol.toByteArray(expansion));
    }

    return options;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private CFReserveOptions options = new CFReserveOptions();

    public Builder withCapacity(long capacity) {
      options.setCapacity(capacity);
      return this;
    }

    public Builder withBucketSize(long bucketSize) {
      options.setBucketSize(bucketSize);
      return this;
    }

    public Builder withMaxIterations(long maxIterations) {
      options.setMaxIterations(maxIterations);
      return this;
    }

    public Builder withExpansion(long expansion) {
      options.setExpansion(expansion);
      return this;
    }

    public CFReserveOptions build() {
      return options;
    }
  }

}
