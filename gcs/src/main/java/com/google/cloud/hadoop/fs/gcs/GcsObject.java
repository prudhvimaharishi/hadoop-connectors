package com.google.cloud.hadoop.fs.gcs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a cached GCS object, holding its chunks in memory.
 * Manages chunk expiry and enforces size limits.
 */
class GcsObject {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final String resourceId;
  // TreeMap key is the starting position of the chunk
  private final TreeMap<Long, GcsObjectChunk> chunks;
  private long currentTotalChunkSize;
  private final long maxChunkBytesPerObject;
  private final long chunkExpiryMillis;

  /**
   * Constructs a GcsObject.
   *
   * @param resourceId Identifier for the GCS object (e.g., path URI string).
   * @param maxChunkBytesPerObject Maximum total bytes of chunk data to store for this object.
   * @param chunkExpiryMillis Duration in milliseconds after which a chunk is considered expired.
   */
  GcsObject(String resourceId, long maxChunkBytesPerObject, long chunkExpiryMillis) {
    this.resourceId = resourceId;
    this.maxChunkBytesPerObject = maxChunkBytesPerObject;
    this.chunkExpiryMillis = chunkExpiryMillis;
    this.chunks = new TreeMap<>();
    this.currentTotalChunkSize = 0;
  }

  /**
   * Attempts to read the requested data range entirely from cached chunks.
   *
   * @param requestedPosition The starting position of the desired data.
   * @param requestedLength The length of the desired data.
   * @return A byte array containing the requested data if fully satisfied from cache, 
   *         otherwise null.
   */
  synchronized byte[] read(long requestedPosition, int requestedLength) {
    removeExpiredChunks(System.currentTimeMillis());

    if (requestedLength == 0) {
      return new byte[0];
    }

    // Find the first chunk that might contain the start of the requested data
    Map.Entry<Long, GcsObjectChunk> floorEntry = chunks.floorEntry(requestedPosition);

    List<GcsObjectChunk> relevantChunks = new ArrayList<>();
    long currentPosition = requestedPosition;
    long endPosition = requestedPosition + requestedLength;

    // Start check from floor chunk if it exists and overlaps
    if (floorEntry != null) {
        GcsObjectChunk chunk = floorEntry.getValue();
        if (!chunk.isExpired(System.currentTimeMillis()) && chunk.getEndPosition() > requestedPosition) {
            if (chunk.getPosition() <= currentPosition && chunk.getEndPosition() >= endPosition) {
                // Single chunk covers the entire request
                relevantChunks.add(chunk);
                currentPosition = endPosition; // Mark as fully covered
            } else if (chunk.getPosition() <= currentPosition && chunk.getEndPosition() > currentPosition) {
                 relevantChunks.add(chunk);
                 currentPosition = chunk.getEndPosition(); // Advance position
            }
        }
    }

    // Continue checking subsequent chunks if needed
    if (currentPosition < endPosition) {
        for (Map.Entry<Long, GcsObjectChunk> entry : chunks.tailMap(currentPosition, true).entrySet()) {
            GcsObjectChunk chunk = entry.getValue();
            if (chunk.isExpired(System.currentTimeMillis())) {
              continue; // Skip expired
            }
            
            // Check for gaps - if the next chunk doesn't start where the last one ended (or at the request start)
            if (chunk.getPosition() > currentPosition) {
                logger.atFinest().log("Cache miss due to gap for %s at position %d", resourceId, currentPosition);
                return null; // Cache miss due to gap
            }

            relevantChunks.add(chunk);
            currentPosition = chunk.getEndPosition();

            if (currentPosition >= endPosition) {
                break; // We have enough data
            }
        }
    }

    // Final check: Do we have all the data required?
    if (currentPosition < endPosition) {
       logger.atFinest().log("Cache miss for %s. Needed pos %d, got up to %d", resourceId, endPosition, currentPosition);
       return null; // Cache miss, didn't get enough contiguous data
    }

    // Assemble the result from relevant chunks
    byte[] result = new byte[requestedLength];
    int resultOffset = 0;
    long copyFromPosition = requestedPosition;

    for (GcsObjectChunk chunk : relevantChunks) {
        long chunkStart = chunk.getPosition();
        long chunkEnd = chunk.getEndPosition();
        byte[] chunkData = chunk.getInternalDataBuffer(); // Use internal buffer for efficiency

        long copyStartInChunk = Math.max(0, copyFromPosition - chunkStart);
        long copyEndInChunk = Math.min(chunkData.length, endPosition - chunkStart);
        int bytesToCopy = (int) (copyEndInChunk - copyStartInChunk);

        if (bytesToCopy > 0) {
            System.arraycopy(chunkData, (int)copyStartInChunk, result, resultOffset, bytesToCopy);
            resultOffset += bytesToCopy;
            copyFromPosition += bytesToCopy;
        }

        if (copyFromPosition >= endPosition) {
            break; // Copied everything needed
        }
    }
    
    if (resultOffset != requestedLength) {
         // This case should ideally not happen if the logic above is correct, but acts as a safeguard.
         logger.atWarning().log("Cache read logic error for %s: Expected %d bytes, copied %d", resourceId, requestedLength, resultOffset);
         return null; 
    }

    logger.atFinest().log("Cache hit for %s at position %d, length %d", resourceId, requestedPosition, requestedLength);
    return result;
  }

  /**
   * Adds chunk data to this object's cache.
   *
   * @param data The byte data to add.
   * @param position The starting position of this data within the object.
   * @param length The length of the data.
   */
  synchronized void addChunkData(byte[] data, long position, int length) {
    if (length == 0) {
        return; // Nothing to add
    }
      
    // Avoid caching zero-length arrays that might be passed
    if (data == null || data.length == 0 || data.length != length) {
        logger.atWarning().log("Invalid data provided for caching at position %d, length %d for %s", 
                             position, length, resourceId);
        return; 
    }
      
    // Check if adding this chunk would immediately exceed the total limit
    // even if it's the only chunk. If so, don't bother adding.
    if (length > maxChunkBytesPerObject && maxChunkBytesPerObject > 0) {
         logger.atFine().log("Chunk length %d exceeds total limit %d for %s. Not caching.", 
                          length, maxChunkBytesPerObject, resourceId);
        return;
    }

    long expiryTimestamp = System.currentTimeMillis() + chunkExpiryMillis;
    GcsObjectChunk newChunk = new GcsObjectChunk(position, Arrays.copyOf(data, length), expiryTimestamp);

    // Naive overlap handling: remove any existing chunk that starts at the same position
    GcsObjectChunk existing = chunks.remove(position);
    if (existing != null) {
        logger.atFinest().log("Replacing existing chunk at position %d for %s", position, resourceId);
        currentTotalChunkSize -= existing.getLength();
    }
    // TODO: Implement more sophisticated overlap/merge logic if needed.

    chunks.put(position, newChunk);
    currentTotalChunkSize += newChunk.getLength();
    logger.atFinest().log("Added chunk for %s at %d, length %d. Total size now: %d",
         resourceId, position, newChunk.getLength(), currentTotalChunkSize);

    enforceSizeLimit();
  }

  /**
   * Removes expired chunks.
   *
   * @param currentTimeMillis The current time to check against expiry timestamps.
   */
  private synchronized void removeExpiredChunks(long currentTimeMillis) {
    Iterator<Map.Entry<Long, GcsObjectChunk>> iterator = chunks.entrySet().iterator();
    int removedCount = 0;
    while (iterator.hasNext()) {
      Map.Entry<Long, GcsObjectChunk> entry = iterator.next();
      GcsObjectChunk chunk = entry.getValue();
      if (chunk.isExpired(currentTimeMillis)) {
        iterator.remove();
        currentTotalChunkSize -= chunk.getLength();
        removedCount++;
      }
    }
    if (removedCount > 0) {
        logger.atFinest().log("Removed %d expired chunks for %s. Total size now: %d", 
                             removedCount, resourceId, currentTotalChunkSize);
    }
  }

  /**
   * Enforces the maximum cache size limit for this object by removing the 
   * oldest chunks (by position) until the limit is met.
   */
  private synchronized void enforceSizeLimit() {
    int removedCount = 0;
    while (currentTotalChunkSize > maxChunkBytesPerObject && !chunks.isEmpty()) {
      // Remove the chunk with the lowest starting position (oldest data assumed)
      Map.Entry<Long, GcsObjectChunk> firstEntry = chunks.firstEntry();
      if (firstEntry != null) {
           GcsObjectChunk removedChunk = chunks.remove(firstEntry.getKey());
           if (removedChunk != null) { // Should always be non-null here
                currentTotalChunkSize -= removedChunk.getLength();
                removedCount++;
           }
      } else {
           // Should not happen if !chunks.isEmpty(), but break defensively
           break; 
      }
    }
    if (removedCount > 0) {
        logger.atFinest().log("Removed %d chunks for size limit (%d bytes) for %s. Total size now: %d",
                             removedCount, maxChunkBytesPerObject, resourceId, currentTotalChunkSize);
    }
  }
  
  @VisibleForTesting
  synchronized long getCurrentTotalChunkSize() {
      return currentTotalChunkSize;
  }
  
  @VisibleForTesting
  synchronized int getChunkCount() {
      return chunks.size();
  }
} 