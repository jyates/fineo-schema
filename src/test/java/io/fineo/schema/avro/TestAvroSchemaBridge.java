package io.fineo.schema.avro;


import io.fineo.internal.customer.FieldNameMap;
import io.fineo.internal.customer.Metadata;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Record;
import io.fineo.schema.store.SchemaStore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNull;

public class TestAvroSchemaBridge {

  /**
   * Test what happens when we don't have any unknown fields and then try to write it out
   *
   * @throws Exception
   */
  @Test
  public void testNoUnknownFieldInRecord() throws Exception {
    String id = "123d43";
    String metricName = "newschema";
    String field = "bField";

    SchemaStore store = SchemaTestUtils.createStoreWithBooleanFields(id, metricName, field);
    // create a simple record with a field the same name of the metric type we created
    Map<String, Object> fields = SchemaTestUtils.getBaseFields(id, metricName);
    fields.put(field, true);
    Record record = new MapRecord(fields);

    //create a bridge between the record and the avro type
    AvroSchemaBridge bridge = SchemaTestUtils.getBridgeForSchema(store, record);
    SchemaTestUtils.readWriteRecord(bridge, record);
  }

  @Test
  public void testCreateWithBadRecords() throws Exception {
    SchemaStore store = Mockito.mock(SchemaStore.class);
    Map<String, Object> content = new HashMap<>();
    MapRecord record = new MapRecord(content);
    assertNull(AvroSchemaBridge.create(store, record));

    content.put(AvroSchemaBridge.ORG_ID_KEY, "orgid");
    assertNull(AvroSchemaBridge.create(store, record));

    content.put(AvroSchemaBridge.ORG_METRIC_TYPE_KEY, "metricid");
    assertNull(
      "Didn't return null when no metadata received from store, even when record had all "
      + "necessary fields",
      AvroSchemaBridge.create(store, record));

    Metadata meta = Mockito.mock(Metadata.class);
    FieldNameMap fields = new FieldNameMap(Collections.emptyMap());
    Mockito.when(store.getSchemaTypes("orgid")).thenReturn(meta);
    Mockito.when(meta.getMetricTypes()).thenReturn(fields);
    assertNull("Didn't return null when org id exists, but metric type not found",
      AvroSchemaBridge.create(store, record));
    Mockito.verify(meta).getMetricTypes();
    Mockito.verify(store, Mockito.times(2)).getSchemaTypes("orgid");
  }
}
