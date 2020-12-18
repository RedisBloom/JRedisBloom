package io.rebloom.client.cf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Interface for RedisBloom Cuckoo Filter Commands
 * 
 * @see <a href=
 *      "https://oss.redislabs.com/redisbloom/Cuckoo_Commands/">RedisBloom
 *      Cuckoo Filter Documentation</a>
 */
public interface Cuckoo {
  /**
   * CF.RESERVE Creates a Cuckoo Filter under key with a single sub-filter for the
   * initial capacity
   * 
   * @param key      The key under which the filter is found
   * @param capacity Estimated capacity for the filter
   */
  void cfCreate(String key, long capacity);

  /**
   * CF.RESERVE Creates a Cuckoo Filter under key with a single sub-filter for the
   * initial capacity and Number of items in each bucket
   * 
   * @param key        The key under which the filter is found
   * @param capacity   Estimated capacity for the filter
   * @param bucketSize Number of items in each bucket
   */
  void cfCreate(String key, long capacity, long bucketSize);

  /**
   * CF.RESERVE Creates a Cuckoo Filter under key with a single sub-filter for the
   * initial capacity, Number of items in each bucket and a Max Number of attempts
   * to swap items between buckets
   * 
   * @param key           The key under which the filter is found
   * @param capacity      Estimated capacity for the filter
   * @param bucketSize    Number of items in each bucket
   * @param maxIterations Number of attempts to swap items between buckets before
   *                      creating an additional filter
   */
  void cfCreate(String key, long capacity, long bucketSize, long maxIterations);

  /**
   * CF.RESERVE Creates a Cuckoo Filter under key with a single sub-filter for the
   * initial capacity, Number of items in each bucket, Max Number of attempts to
   * swap items between buckets ad expansion multiplier
   * 
   * @param key           The key under which the filter is found
   * @param capacity      Estimated capacity for the filter
   * @param bucketSize    Number of items in each bucket
   * @param maxIterations Number of attempts to swap items between buckets before
   *                      creating an additional filter
   * @param expansion     When a new filter is created, its size is the size of
   *                      the current filter multiplied by expansion
   */
  void cfCreate(String key, long capacity, long bucketSize, long maxIterations, long expansion);

  /**
   * CF.ADD Adds an item to the cuckoo filter, creating the filter if it does not
   * exist
   * 
   * @param key  The name of the filter
   * @param item The item to add
   * @return true on success, false otherwise
   */
  boolean cfAdd(String key, String item);

  /**
   * CF.ADDNX Adds an item to the cuckoo filter, only if it does not exist yet
   * 
   * @param key  The name of the filter
   * @param item The item to add
   * @return true if the item was added to the filter, false if the item already
   *         exists.
   */
  boolean cfAddNx(String key, String item);

  /**
   * CF.INSERT Adds one or more items to a cuckoo filter, creating it if it does
   * not exist yet.
   * 
   * @param key   The name of the filter
   * @param items One or more items to add
   * @return true if the item was successfully inserted, false if an error
   *         occurred
   */
  List<Boolean> cfInsert(String key, String... items);

  /**
   * CF.INSERT Adds one or more items to a cuckoo filter, allowing the filter to
   * be created with a custom capacity if it does not exist yet.
   * 
   * @param key      The name of the filter
   * @param capacity Specifies the desired capacity of the new filter, if this
   *                 filter does not exist yet. If the filter already exists, then
   *                 this parameter is ignored.
   * @param items    One or more items to add
   * @return true if the item was successfully inserted, false if an error
   *         occurred
   */
  List<Boolean> cfInsert(String key, long capacity, String... items);

  /**
   * CF.INSERT Adds one or more items to a cuckoo filter. If the specified filer
   * does not exists, false is returned to signify an error.
   * 
   * @param key   The name of the filter
   * @param items One or more items to add
   * @return true if the item was successfully inserted, false if an error
   *         occurred
   */
  List<Boolean> cfInsertNoCreate(String key, String... items);

  /**
   * CF.INSERTNX Adds one or more items to a cuckoo filter, only if it does not
   * exist yet
   * 
   * @param key   The name of the filter
   * @param items One or more items to add
   * @return true if the item was added to the filter, false if the item already
   *         exists.
   */
  List<Boolean> cfInsertNx(String key, String... items);

  /**
   * CF.INSERTNX Adds one or more items to a cuckoo filter, only if it does not
   * exist yet
   * 
   * @param key      The name of the filter
   * @param capacity Specifies the desired capacity of the new filter, if this
   *                 filter does not exist yet. If the filter already exists, then
   *                 this parameter is ignored.
   * @param items    One or more items to add
   * @return true if the item was added to the filter, false if the item already
   *         exists.
   */
  List<Boolean> cfInsertNx(String key, long capacity, String... items);

  /**
   * CF.INSERTNX Adds one or more items to a cuckoo filter, only if it does not
   * exist yet
   * 
   * @param key   The name of the filter
   * @param items One or more items to add
   * @return true if the item was added to the filter, false if the item already
   *         exists.
   */
  List<Boolean> cfInsertNxNoCreate(String key, String... items);

  /**
   * CF.EXISTS Check if an item exists in a Cuckoo Filter
   * 
   * @param key  The name of the filter
   * @param item The item to check for
   * @return false if the item certainly does not exist, true if the item may
   *         exist.
   */
  boolean cfExists(String key, String item);

  /**
   * CF.DEL Deletes an item once from the filter. If the item exists only once, it
   * will be removed from the filter. If the item was added multiple times, it
   * will still be present.
   * 
   * @param key  The name of the filter
   * @param item The item to delete from the filter
   * @return true if the item has been deleted, false if the item was not found.
   */
  boolean cfDel(String key, String item);

  /**
   * CF.COUNT Returns the number of times an item may be in the filter.
   * 
   * @param key  The name of the filter
   * @param item The item to count
   * @return The number of times the item exists in the filter
   */
  long cfCount(String key, String item);

  /**
   * CF.SCANDUMP Begins an incremental save of the cuckoo filter. This is useful
   * for large cuckoo filters which cannot fit into the normal SAVE and RESTORE
   * model.
   * 
   * The Iterator is passed as input to the next invocation of SCANDUMP . If
   * Iterator is 0, the iteration has completed.
   * 
   * @param key      Name of the filter
   * @param iterator This is either 0, or the iterator from a previous invocation
   *                 of this command
   * @return an IteratorDataPair containing the Iterator and Data.
   */
  IteratorDataPair cfScanDump(String key, long iterator);

  /**
   * CF.SCANDUMP Begins an incremental save of the cuckoo filter. This is useful
   * for large cuckoo filters which cannot fit into the normal SAVE and RESTORE
   * model.
   * 
   * Returns an iterator over the IteratorDataPairs in proper sequence.
   * 
   * @param key Name of the filter
   * @return Returns an iterator over the IteratorDataPairs containing the chunks
   *         of byte[] representing the filter
   */
  Iterator<IteratorDataPair> cfScanDumpIterator(String key);

  /**
   * CF.SCANDUMP Begins an incremental save of the cuckoo filter. This is useful
   * for large cuckoo filters which cannot fit into the normal SAVE and RESTORE
   * model.
   * 
   * Returns a sequential Stream with the IteratorDataPairs as its source.
   * 
   * @return Returns a sequential Stream of IteratorDataPairs
   */
  Stream<IteratorDataPair> cfScanDumpStream(String key);

  /**
   * CF.LOADCHUNK Restores a filter previously saved using SCANDUMP . See the
   * SCANDUMP command for example usage.
   * 
   * @param key Name of the filter to restore
   * @param idp an IteratorDataPair containing the Iterator and Data.
   */
  void cfLoadChunk(String key, IteratorDataPair idp);

  /**
   * CF.INFO Return information about filter
   * 
   * @param key Name of the filter to restore
   * @return A Map containing Size, Number of buckets, Number of filter, Number of
   *         items inserted, Number of items deleted, Bucket size, Expansion rate,
   *         Max iteration
   */
  Map<String, Long> cfInfo(String key);
}
