package com.google.cloud.hadoop.fs.gcs;

import com.google.common.flogger.GoogleLogger;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

/**
 * Wraps a GoogleHadoopFSInputStream to add an in-memory caching layer.
 * Reads are first attempted from the cache; if missed, the read is delegated
 * to the underlying stream, and the result is added to the cache.
 */
class CachedGoogleHadoopFsInputStream extends FSInputStream implements IOStatisticsSource {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final GoogleHadoopFSInputStream underlyingStream;
  private final GcsObjectStore cache;
  private final StorageResourceId resourceId;
  private boolean closed;

  // Used for single-byte reads
  private final byte[] singleReadBuf = new byte[1];

  /**
   * Constructs a CachedGoogleHadoopFsInputStream.
   *
   * @param underlyingStream The original input stream to wrap.
   * @param cache The shared GcsObjectStore instance.
   * @param resourceId The unique identifier for the file being read (e.g., path string).
   */
  CachedGoogleHadoopFsInputStream(
      GoogleHadoopFSInputStream underlyingStream,
      GcsObjectStore cache,
      StorageResourceId resourceId) {
    this.underlyingStream = underlyingStream;
    this.cache = cache;
    this.resourceId = resourceId;
    this.closed = false;
     logger.atFiner().log("CachedGoogleHadoopFsInputStream created for %s", resourceId.toString());
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
    // Delegate single-byte reads to the multi-byte read method for simplicity
    // and potential caching benefit if read(buf, off, len) is called.
    int bytesRead = read(singleReadBuf, 0, 1);
    if (bytesRead == -1) {
      return -1; // EOF
    }
    return singleReadBuf[0] & 0xFF;
  }

  @Override
  public synchronized int read(byte[] buf, int offset, int length) throws IOException {
    checkNotClosed();
    if (length == 0) {
      return 0;
    }
    validatePositionedReadArgs(getPos(), buf, offset, length);

    long currentPos = getPos();
    GcsObject object = cache.getObject(resourceId);
    byte[] cachedData = object.read(currentPos, length);

    if (cachedData != null) {
      // Cache Hit
      logger.atFinest().log("Cache HIT for %s at position %d, length %d", resourceId.toString(), currentPos, length);
      System.arraycopy(cachedData, 0, buf, offset, length);
      // Seek the underlying stream forward to maintain position consistency
      long targetPos = currentPos + length;
      underlyingStream.seek(targetPos);
      return length;
    } else {
      // Cache Miss
      logger.atFinest().log("Cache MISS for %s at position %d, length %d", resourceId.toString(), currentPos, length);
      // Must ensure the underlying stream is at the correct position before reading
      // (It should be, unless seek was called externally without our wrapper knowledge - unlikely)
      if (underlyingStream.getPos() != currentPos) {
          logger.atWarning().log("Underlying stream position mismatch for %s. Expected %d, got %d. Seeking.",
                                resourceId.toString(), currentPos, underlyingStream.getPos());
          underlyingStream.seek(currentPos);
      }
      
      int bytesRead = underlyingStream.read(buf, offset, length);

      if (bytesRead > 0) {
        // Add the newly read data to the cache
        byte[] dataToCache = new byte[bytesRead];
        System.arraycopy(buf, offset, dataToCache, 0, bytesRead);
        cache.put(resourceId, dataToCache, currentPos, bytesRead);
      }
      return bytesRead;
    }
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkNotClosed();
    if (pos < 0) {
       throw new EOFException("Cannot seek to negative position: " + pos);
    }
    logger.atFiner().log("Seek to %d for %s", pos, resourceId.toString());
    // Delegate seek to the underlying stream
    underlyingStream.seek(pos);
    // Note: We could potentially add cache prefetching logic here if desired.
  }

  @Override
  public synchronized long getPos() throws IOException {
    checkNotClosed();
    return underlyingStream.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    checkNotClosed();
    // Delegate - generally not supported/effective for object stores
    return underlyingStream.seekToNewSource(targetPos);
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      logger.atFiner().log("Closing cached stream for %s", resourceId.toString());
      underlyingStream.close();
      closed = true;
      // Note: We don't explicitly remove the object from the GcsObjectStore here,
      // as it manages its own LRU lifecycle.
    }
  }

  @Override
  public synchronized int available() throws IOException {
      checkNotClosed();
      // The concept of 'available' without blocking is tricky with network streams
      // and caching. Delegating to the underlying stream is a reasonable default,
      // but its accuracy might be limited.
      return underlyingStream.available();
  }

  /**
   * Get the IOStatistics provided by the underlying stream.
   * Note: This currently does not include cache-specific statistics.
   *
   * @return IOStatistics from the wrapped stream.
   */
  @Override
  public IOStatistics getIOStatistics() {
    // Return stats from the underlying stream. Cache stats are not yet integrated.
    return underlyingStream.getIOStatistics();
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream closed for resource: " + resourceId.toString());
    }
  }

  /**
   * Validates the arguments for a positioned read operation.
   * Copied from org.apache.hadoop.fs.FSInputStream.
   */
  protected void validatePositionedReadArgs(long position, byte[] buffer,
      int offset, int length) throws EOFException {
    if (position < 0) {
      throw new IllegalArgumentException("Position is negative: " + position);
    }
    if (buffer == null) {
      throw new NullPointerException("Null buffer passed");
    }
    if (offset < 0) {
      throw new IndexOutOfBoundsException("Negative offset: " + offset);
    }
    if (length < 0) {
      throw new IndexOutOfBoundsException("Negative length: " + length);
    }
    if (length > buffer.length - offset) {
      throw new IndexOutOfBoundsException(
          "Requested length " + length
          + " exceeds buffer size " + buffer.length
          + " with offset " + offset);
    }
  }
} 