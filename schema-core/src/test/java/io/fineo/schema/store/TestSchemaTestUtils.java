package io.fineo.schema.store;

import io.fineo.internal.customer.BaseFields;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Because sometimes the tools get too complicated
 */
public class TestSchemaTestUtils {

  @Test
  public void testCreateRandomRecord() throws Exception {
    GenericRecord record = SchemaTestUtils.createRandomRecord();
    BaseFields base = (BaseFields) record.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    assertTrue(base.getTimestamp() > 0);
    assertTrue(base.getWriteTime() > 0);
    assertEquals(newHashMap(), base.getUnknownFields());
  }
}
