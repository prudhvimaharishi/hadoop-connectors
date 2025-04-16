package com.google.cloud.hadoop.fs.gcs;

import java.util.Arrays;

/**
 * Represents a chunk of data cached from a GCS object.
 * This class is intended for internal use by GcsObject.
 */
class GcsObjectChunk {

  private final long position;
  private final byte[] data; // Contains the actual chunk data
  private final long expiryTimestamp; // System.currentTimeMillis() + expiry duration

  /**
   * Constructs a GcsObjectChunk.
   *
   * @param position The starting byte offset of this chunk within the original GCS object.
   * @param data The byte data of this chunk. A copy is made internally.
   * @param expiryTimestamp The absolute time (in milliseconds since epoch) when this chunk expires.
   */
  GcsObjectChunk(long position, byte[] data, long expiryTimestamp) {
    this.position = position;
    // Defensive copy
    this.data = Arrays.copyOf(data, data.length);
    this.expiryTimestamp = expiryTimestamp;
  }

  long getPosition() {
    return position;
  }

  int getLength() {
    return data.length;
  }

  byte[] getData() {
    // Return a copy to prevent external modification
    return Arrays.copyOf(data, data.length);
  }
  
  /**
   * Gets the internal data buffer without copying. Use with caution.
   * @return the internal byte array.
   */
  byte[] getInternalDataBuffer() {
      return data;
  }

  long getExpiryTimestamp() {
    return expiryTimestamp;
  }

  /**
   * Checks if this chunk has expired based on the provided current time.
   *
   * @param currentTimeMillis The current time in milliseconds since the epoch.
   * @return true if the chunk is expired, false otherwise.
   */
  boolean isExpired(long currentTimeMillis) {
    return currentTimeMillis >= expiryTimestamp;
  }

  /**
   * Returns the end position of this chunk (exclusive).
   * @return The position of the byte immediately after the last byte in this chunk.
   */
  long getEndPosition() {
    return position + data.length;
  }
} 