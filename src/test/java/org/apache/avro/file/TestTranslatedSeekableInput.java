package org.apache.avro.file;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestTranslatedSeekableInput {

  @Test
  public void test() throws Exception {
    byte[] data = new byte[128];
    for (byte i = 0; i < 127; i++) {
      data[i] = i;
    }

    SeekableByteArrayInput input = new SeekableByteArrayInput(data);
    /** [0*] 1 2 3 4 */
    TranslatedSeekableInput trans = new TranslatedSeekableInput(0, 1, input);
    assertEquals(1, trans.length());
    byte[] bit = new byte[1];
    trans.read(bit, 0, 1);
    assertEquals(0, bit[0]);
    assertEquals(127, trans.remainingTotal());

    trans.seek(2);
    assertEquals("Seeking past end of limited does not return -1 on read", -1, trans.read(bit, 0, 1));

    // seek back to beginning
    trans.seek(0);
    /** [0*] 1 2 3 4 */
    // move forward two bytes
    trans.nextBlock(2);
    /** 0 [1* 2] 3 4 */
    trans.read(bit, 0, 1);
    /** 0 [1 2*] 3 4 */
    assertEquals(1, bit[0]);
    assertEquals(126, trans.remainingTotal());

    // read more bytes than we are currently supporting
    byte[] next = new byte[130];
    assertEquals(1, trans.read(next, 0, next.length));

    trans.nextBlock(2);
    assertEquals(2, trans.read(next, 1, 2));
  }
}
