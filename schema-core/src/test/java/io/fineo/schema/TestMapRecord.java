package io.fineo.schema;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestMapRecord {

  @Test
  public void testCastingStrings() throws Exception {
    Map<String, Object> map = new HashMap<>();
    String field = "key";
    map.put(field, "1");

    MapRecord record = new MapRecord(map);
    assertEquals(1, (int)record.getIntegerByField(field));
    assertEquals(1.0f, record.getFloatByFieldName(field), 0);
    assertEquals(1.0, record.getDoubleByFieldName(field), 0);
    assertEquals(1L, (long)record.getLongByFieldName(field));
  }
}
