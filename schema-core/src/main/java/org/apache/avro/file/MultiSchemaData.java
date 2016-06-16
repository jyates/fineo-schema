package org.apache.avro.file;

/**
 *
 */
public class MultiSchemaData {
  private static final int VERSION = 1;
  public static final byte[] MAGIC = new byte[]{(byte) '1', (byte) 'c', (byte) 'k', VERSION};
  public static final int OFFSET_COUNT_LENGTH = 4;

  private MultiSchemaData(){
    //private ctor for util class
  }
}
