package org.apache.avro.file;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestSeekableByteBufferInput {

  @Test
  public void testReadAndTell() throws Exception {
    ByteBuffer data = data();
    SeekableByteBufferInput input = SeekableByteBufferInput.create(data);
    read(input, 1, 0);
    assertEquals(1, input.tell());
    read(input, 1, 1);
    assertEquals(2, input.tell());
    input.seek(0);
    read(input, 1, 0);
    assertEquals(1, input.tell());

    //read into the array
    byte[] read = new byte[]{7, 7, 7, 0};
    input.seek(0);
    assertEquals(1, input.read(read, 3, 1));
    assertEquals(1, input.tell());
  }

  /**
   * Total length does not change, regardless of where you are in the data
   *
   * @throws Exception on failure
   */
  @Test
  public void testLength() throws Exception {
    SeekableByteBufferInput input = SeekableByteBufferInput.create(data());
    length(input);
    read(input, 1, 0);
    length(input);
    input.seek(10);
    length(input);
  }

  private void length(SeekableByteBufferInput input) throws IOException {
    assertEquals(128, input.length());
  }

  private void read(SeekableByteBufferInput input, int bytes, byte[] expected) throws IOException {
    byte[] b = new byte[bytes];
    assertEquals(expected.length, input.read(b, 0, bytes));
    assertArrayEquals(expected, b);
  }

  private void read(SeekableByteBufferInput input, int bytes, int expected) throws IOException {
    read(input, bytes, new byte[]{(byte) expected});
  }

  /**
   * Test the end range behavior, when we should be returning -1 (no more data) and how we manage
   * reading past the 'end' of the data
   *
   * @throws Exception on failure
   */
  @Test
  public void testEndRangeSeek() throws Exception {
    ByteBuffer data = data();
    SeekableByteBufferInput input = SeekableByteBufferInput.create(data);
    byte[] expected = new byte[127];
    System.arraycopy(data.array(), 0, expected, 0, 127);
    read(input, 127, expected);

    expected = new byte[]{127};
    byte[] remaining = new byte[1];
    assertEquals(1, input.read(remaining, 0, 1));
    assertArrayEquals(expected, remaining);
    assertEquals(-1, input.read(remaining, 0, 1));

    // rewind and read again
    input.seek(127);
    assertEquals(1, input.read(remaining, 0, 1));
    assertArrayEquals(expected, remaining);
    assertEquals(-1, input.read(remaining, 0, 1));

    // read 'past' the end
    input.seek(127);
    assertEquals(1, input.read(remaining, 0, 2));
    assertArrayEquals(expected, remaining);
    assertEquals(-1, input.read(remaining, 0, 1));

    // and read again, but don't seek, which shouldn't do anything
    remaining = new byte[0];
    assertEquals(-1, input.read(remaining, 0, 1));
  }

  ByteBuffer data() {
    byte[] data = new byte[128];
    for (int i = 0; i <= 127; i++) {
      data[i] = (byte) i;
    }
    return ByteBuffer.wrap(data);
  }
}