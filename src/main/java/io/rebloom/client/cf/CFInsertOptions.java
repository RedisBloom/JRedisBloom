package io.rebloom.client.cf;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class CFInsertOptions {
  private long capacity = -1; // optional
  private boolean noCreate = false; // optional

  protected CFInsertOptions() {
  }

  public long getCapacity() {
    return capacity;
  }

  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }

  public boolean isNoCreate() {
    return noCreate;
  }

  public void setNoCreate(boolean noCreate) {
    this.noCreate = noCreate;
  }

  public List<byte[]> asListOfByteArrays() {
    List<byte[]> options = new ArrayList<>();
    
    if (capacity != -1) {
      options.add(SafeEncoder.encode("CAPACITY"));
      options.add(Protocol.toByteArray(capacity));
    }

    if (noCreate) {
      options.add(SafeEncoder.encode("NOCREATE"));
    }

    return options;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private CFInsertOptions options = new CFInsertOptions();

    public Builder withCapacity(long capacity) {
      options.setCapacity(capacity);
      return this;
    }

    public Builder noCreate() {
      options.setNoCreate(true);
      return this;
    }

    public CFInsertOptions build() {
      return options;
    }
  }

}
