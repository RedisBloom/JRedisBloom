package io.rebloom.client.td;

import java.util.Map;

public interface TDigest {

  void tdigestCreate(String key, int compression);

  void tdigestReset(String key);

  void tdigestMerge(String toKey, String fromKey);

  Map<String, Object> tdigestInfo(String key);

  void tdigestAdd(String key, TDigestValueWeight... valueWeights);

  double tdigestCDF(String key, double value);

  Map<Double, Double> tdigestQuantile(String key, double... quantile);

  double tdigestMin(String key);

  double tdigestMax(String key);
}
