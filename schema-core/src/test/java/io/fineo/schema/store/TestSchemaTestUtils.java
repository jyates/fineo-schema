package io.fineo.schema.store;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Because sometimes the tools get too complicated
 */
public class TestSchemaTestUtils {

  @Test
  public void testCreateRandomRecord() throws Exception {
    SchemaTestUtils.createRandomRecord();
  }
}
