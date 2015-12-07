package org.apache.avro.file;

import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * A seekable input that limits how much of the actual input stream is actually exposed (to
 * prevent rebuffering the same data) and supports reading from a fixed offset in the input stream
 */
public class TranslatedSeekableInput implements SeekableInput {
  private long length;
  private final SeekableInput delegate;
  private long offset;

  public TranslatedSeekableInput(long offset, long length, SeekableInput input) {
    this.length = length;
    this.delegate = input;
    this.offset = offset;
  }

  /**
   * Move the input forward to the next 'chunk'. Sets the offset (start) to the current length
   * and sets the length to the specified length. So if the input was [0, 10] of a total 16bytes
   * and then you moved forward by 6, the new range would be [10, 16].
   *
   * @param length move the read forward to the next length
   * @throws IOException
   */
  public void moveForward(long length) throws IOException {
    this.offset = this.length;
    this.length = Math.min(this.length + length, delegate.length());
    this.delegate.seek(offset);
  }

  public long remainingTotal() throws IOException {
    return this.delegate.length() - this.delegate.tell();
  }

  @Override
  public void seek(long p) throws IOException {
    delegate.seek(p + offset);
  }

  @Override
  public long tell() throws IOException {
    return delegate.tell() - offset;
  }

  @Override
  public long length() throws IOException {
    return length - offset;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    // avro tries to be smart and 'compact' the buffer by reading 8192 bytes at once. This
    // seeks us waaaaay past the end of the delegate, so we have to limit the length of the
    // read by the length we support out of the buffer
    long remaining = length - tell();
    if (remaining == 0) {
      return -1;
    }
    return delegate.read(b, off, (int) Math.min((long) len, remaining));
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
