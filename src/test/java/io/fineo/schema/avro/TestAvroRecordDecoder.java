package io.fineo.schema.avro;

import com.google.common.collect.Lists;
import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Record;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test some standard user flows so they can interact with record and the store correctly +
 * naturally.
 */
public class TestAvroRecordDecoder {

  @Test
  public void testSerDe() throws Exception {
    String orgID = "123d43";
    String metricName = "newschema";
    String field = "bField";

    SchemaStore store = SchemaTestUtils.createStoreWithBooleanFields(orgID, metricName, field);
    // create a simple record with a field the same name of the metric type we created
    Map<String, Object> fields = SchemaTestUtils.getBaseFields(orgID, metricName);
    fields.put(field, true);
    Record record = new MapRecord(fields);

    //create a bridge between the record and the avro type
    AvroSchemaBridge bridge = SchemaTestUtils.getBridgeForSchema(store, record);
    GenericRecord deserialized = SchemaTestUtils.readWriteRecord(bridge, record);
    AvroRecordDecoder decoder = new AvroRecordDecoder(deserialized);
    AvroRecordDecoder.RecordMetadata metadata = decoder.getMetadata();
    assertEquals(orgID, metadata.orgID);

    // ensure the canonical name matches what we have in the store
    Metadata schemas = store.getSchemaTypes(orgID);
    Map<String, List<String>> metricAliasMap =
      schemas.getMetricTypes().getCanonicalNamesToAliases();
    assertEquals(1, metricAliasMap.size());
    assertEquals(metricAliasMap.keySet().iterator().next(), metadata.metricCannonicalType);
    assertEquals(Lists.newArrayList(metricName), metricAliasMap.get(metadata.metricCannonicalType));

    // ensure the base fields match as we expect from the record
    BaseFields baseFields = decoder.getBaseFields();
    // only time outside the schema that we actually reference this by name
    assertEquals(fields.get(AvroSchemaBridge.BASE_TIMESTAMP_FIELD_NAME), baseFields.getTimestamp());
    assertEquals(metricName, baseFields.getAliasName());
    assertEquals(0, baseFields.getUnknownFields().size());

    // verify that we have the added boolean field
    Metric metric = store.getMetricMetadata(metadata.orgID, metadata.metricCannonicalType);
    // canonical name has the aliased field name
    Map<String, List<String>> aliases =
      metric.getMetadata().getMetricTypes().getCanonicalNamesToAliases();
    assertEquals(2, aliases.size());
    assertEquals(new ArrayList<>(0), aliases.remove(AvroSchemaBridge.BASE_FIELDS_KEY));
    String fieldCannonicalName = aliases.keySet().iterator().next();
    assertEquals(Lists.newArrayList(field), aliases.get(fieldCannonicalName));
    // its the right type

    Schema s = metadata.metricSchema;
    Schema.Field f = s.getField(fieldCannonicalName);
    assertNotNull(f);
    assertEquals("boolean", f.schema().getType().getName());
  }
}