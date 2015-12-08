package io.fineo.schema.avro;


import io.fineo.internal.customer.Metadata;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Record;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;

import static org.junit.Assert.assertNotNull;

public class TestAvroSchemaBridge {

  /**
   * Test what happens when we don't have any unknown fields and then try to write it out
   * @throws Exception
   */
  @Test
  public void testNoUnknownFieldInRecord() throws Exception{
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
}
