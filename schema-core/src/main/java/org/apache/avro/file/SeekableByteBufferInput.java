package org.apache.avro.file;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link SeekableInput} that lets you easy seek around a ByteBuffer. Note, using this class
 * directly <b>will not change the mark</b> for the {@link java.nio.ByteBuffer}. However, any
 * changes to the underlying bytes in the passed {@link ByteBuffer} will be reflected when reading.
 */
public class SeekableByteBufferInput implements SeekableInput {
  private final ByteBuffer buff;

  public SeekableByteBufferInput(ByteBuffer buffer) {
    this.buff = buffer;
    buff.rewind();
  }

  @Override
  public void seek(long p) throws IOException {
    buff.position((int) p);
  }

  @Override
  public long tell() throws IOException {
    return buff.position();
  }

  @Override
  public long length() throws IOException {
    return buff.limit();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (buff.remaining() == 0) {
      return -1;
    }

    int read = Math.min(buff.remaining(), len);
    buff.get(b, off, read);
    return read;
  }

  @Override
  public void close() throws IOException {
    //noop
  }

  public static SeekableByteBufferInput create(ByteBuffer buff) {
    return new SeekableByteBufferInput(buff.duplicate());
  }
}
