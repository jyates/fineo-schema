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
    TranslatedSeekableInput trans = new TranslatedSeekableInput(0, 1, input);
    assertEquals(1, trans.length());

    byte[] bit = new byte[1];
    trans.read(bit, 0, 1);
    assertEquals(0, bit[0]);
    assertEquals(127, trans.remainingTotal());

    trans.seek(2);
    try {
      trans.read(bit, 0, 1);
      fail("Should be outside the tranlated bounds");
    } catch (IndexOutOfBoundsException e) {
      //expected
    }

    // seek back to beginning
    trans.seek(0);
    // move forward two bytes
    trans.moveForward(2);
    trans.read(bit, 0, 1);
    assertEquals(1, bit[0]);
    assertEquals(126, trans.remainingTotal());
  }
}
