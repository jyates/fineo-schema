package org.apache.avro.file;

import java.io.IOException;

/**
 * A seekable input that limits how much of the actual input stream is actually exposed (to
 * prevent rebuffering the same data) and supports reading from a fixed offset in the input stream
 */
public class TranslatedSeekableInput implements SeekableInput {
  /**
   * limit inside the delegate record up to which we can read
   */
  private long limit;
  private final SeekableInput delegate;
  /** offset into the delegate record that we start reading */
  private long offset;

  public TranslatedSeekableInput(long offset, long limit, SeekableInput input) throws IOException {
    this.limit = limit;
    this.delegate = input;
    this.offset = offset;
    // inherently the input cannot start earlier than this point, so we seek back to the
    // beginning to make it easier to reason about, especially if someone is trying to read directly
    input.seek(offset);
  }

  /**
   * Move the input forward to the next 'chunk'. Sets the offset (start) to the current limit
   * and moves the limit up by to the specified amount. So if the input was [0, 10] of a total 16bytes
   * and then you moved forward by 6, the new range would be [10, 16].
   *
   * @param length amount to move up the limit
   * @throws IOException
   */
  public void nextBlock(long length) throws IOException {
    this.offset = this.limit;
    this.limit = Math.min(this.limit + length, delegate.length());
  }

  /**
   * @return Total remaining bytes in the underlying input stream from the current read point
   * @throws IOException if there is an error accessing the delegate
   */
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
    return limit - offset;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    // just incase the input is seeked to before the current offset. Corrects the current
    // delegate tell location as well to start at the offset, it the input stream is seeked earlier.
    if (delegate.tell() < offset) {
      this.delegate.seek(offset);
    }
    // ensure we don't read past the current end of the buffer
    long remaining = this.length() - this.tell();
    if (remaining <= 0) {
      return -1;
    }
    int readAmount = (int) Math.min(remaining, len);
    return delegate.read(b, off, readAmount);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
