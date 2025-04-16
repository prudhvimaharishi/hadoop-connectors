package com.google.cloud.hadoop.fs.gcs;

import com.google.common.flogger.GoogleLogger;
import com.google.cloud.hadoop.gcsio.StorageResourceId;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A store for GcsObject instances, implementing an LRU eviction policy based on access order.
 * This class is thread-safe.
 */
class GcsObjectStore {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final LinkedHashMap<StorageResourceId, GcsObject> objectCache;
  private final int maxObjects;
  private final long maxChunkBytesPerObject;
  private final long chunkExpiryMillis;

  /**
   * Constructs a GcsObjectStore.
   *
   * @param maxObjects The maximum number of GcsObject instances to hold in the cache.
   * @param maxChunkBytesPerObject The maximum size (in bytes) of cached chunks per object.
   * @param chunkExpiryMillis The expiry time (in milliseconds) for individual chunks.
   */
  GcsObjectStore(int maxObjects, long maxChunkBytesPerObject, long chunkExpiryMillis) {
    this.maxObjects = maxObjects;
    this.maxChunkBytesPerObject = maxChunkBytesPerObject;
    this.chunkExpiryMillis = chunkExpiryMillis;

    // Initialize LinkedHashMap for LRU behavior (accessOrder = true)
    // Initial capacity is maxObjects + 1 to avoid immediate resizing, load factor 0.75f is default
    this.objectCache = new LinkedHashMap<StorageResourceId, GcsObject>(maxObjects + 1, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<StorageResourceId, GcsObject> eldest) {
        // Remove the eldest entry if the size exceeds the maximum number of objects
        boolean remove = size() > GcsObjectStore.this.maxObjects;
        if (remove) {
            logger.atFine().log("Evicting LRU object %s from cache (limit %d reached)",
                               eldest.getKey().toString(), GcsObjectStore.this.maxObjects);
        }
        return remove;
      }
    };
     logger.atInfo().log("GcsObjectStore initialized with maxObjects=%d, maxChunkBytesPerObject=%d, chunkExpiryMillis=%d",
                        maxObjects, maxChunkBytesPerObject, chunkExpiryMillis);
  }

  /**
   * Retrieves a GcsObject from the cache or creates it if it doesn't exist.
   * Accessing an object marks it as recently used.
   *
   * @param resourceId The identifier of the GcsObject.
   * @return The existing or newly created GcsObject.
   */
  synchronized GcsObject getObject(StorageResourceId resourceId) {
    // computeIfAbsent ensures atomic creation and insertion if the key is missing
    // It also updates the access order on retrieval if the key exists.
    return objectCache.computeIfAbsent(resourceId, id -> {
        logger.atFine().log("Creating new GcsObject in cache for %s", id.toString());
        return new GcsObject(id.toString(), maxChunkBytesPerObject, chunkExpiryMillis);
    });
  }

  /**
   * Adds data to a specific object in the cache.
   * If the object doesn't exist, it will be created.
   *
   * @param resourceId The identifier of the object.
   * @param data The byte data to cache.
   * @param position The starting position of the data within the object.
   * @param length The length of the data.
   */
  synchronized void put(StorageResourceId resourceId, byte[] data, long position, int length) {
    if (data == null || length == 0) {
        return; // Don't cache empty/null data
    }
    // Get the object (creates if needed, updates access order)
    GcsObject object = getObject(resourceId);
    // Add the chunk data to the object (object handles internal size limits/expiry)
    object.addChunkData(data, position, length);
  }
  
  /**
   * Clears the entire object cache.
   */
  synchronized void clear() {
      objectCache.clear();
      logger.atInfo().log("GcsObjectStore cache cleared.");
  }
  
   /**
   * Gets the current number of objects in the cache.
   * @return the number of GcsObject instances currently stored.
   */
  synchronized int getCurrentObjectCount() {
      return objectCache.size();
  }
} 