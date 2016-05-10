package io.fineo.schema.avro;

import com.google.common.collect.Lists;
import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Record;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestRecordMetadata {

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

    // encode the record as an avro record
    AvroSchemaEncoder bridge = SchemaTestUtils.getBridgeForSchema(store, record);
    GenericRecord deserialized = SchemaTestUtils.writeReadRecord(bridge, record);

    // validate that we parse elements out correctly from the record
    verifyRecordMetadataMatchesExpectedNaming(deserialized);

    // From here it gets a little bit more complicated as we have to compare what we to the
    // record fields to vs. the alias names. That means a bit more hoops in terms of looking up
    // aliases etc. in the Metric/Metadata.

    // ensure the canonical name matches what we have in the store
    RecordMetadata metadata = RecordMetadata.get(deserialized);
    Metadata schemas = store.getOrgMetadata(orgID);
    Map<String, List<String>> metricAliasMap =
      schemas.getCanonicalNamesToAliases();
    assertEquals(1, metricAliasMap.size());
    assertEquals(metricAliasMap.keySet().iterator().next(), metadata.metricCanonicalType);
    assertEquals(Lists.newArrayList(metricName), metricAliasMap.get(metadata.metricCanonicalType));

    // ensure the base fields match as we expect from the record
    BaseFields baseFields = metadata.getBaseFields();
    // only time outside the schema that we actually reference this by name
    assertEquals(fields.get(AvroSchemaEncoder.BASE_TIMESTAMP_FIELD_NAME),
      baseFields.getTimestamp());
    assertEquals(metricName, baseFields.getAliasName());
    assertEquals(0, baseFields.getUnknownFields().size());

    // verify that we have the added boolean field
    Metric metric = store.getMetricMetadata(metadata.orgID, metadata.metricCanonicalType);
    // canonical name has the aliased field name
    Map<String, List<String>> aliases =
      metric.getMetadata().getCanonicalNamesToAliases();
    assertEquals(2, aliases.size());
    assertEquals(new ArrayList<>(0), aliases.remove(AvroSchemaEncoder.BASE_FIELDS_KEY));
    String fieldCannonicalName = aliases.keySet().iterator().next();
    assertEquals(Lists.newArrayList(field), aliases.get(fieldCannonicalName));
    // its the right type

    Schema s = metadata.metricSchema;
    Schema.Field f = s.getField(fieldCannonicalName);
    assertNotNull(f);
    SchemaTestUtils.verifyFieldType("boolean", f);
  }

  /**
   * Base fields can either be encoded as a GenericRecord or as the typed BaseFields, in which case
   * it deserializes differently, so we have this extra test method to catch that other case
   *
   * @throws Exception
   */
  @Test
  public void testDeserializationOfUnserializedRecord() throws Exception {
    String id = "123d43";
    String metricName = "newschema";
    String field = "bField";
    long start = 1;

    SchemaStore store = SchemaTestUtils.createStoreWithBooleanFields(id, metricName, field);
    // create a simple record with a field the same name of the metric type we created
    Map<String, Object> rawFields = SchemaTestUtils.getBaseFields(id, metricName, start);
    rawFields.put(field, true);
    Record genRecord = new MapRecord(rawFields);

    //create a bridge between the record and the avro type
    AvroSchemaEncoder bridge = SchemaTestUtils.getBridgeForSchema(store, genRecord);
    GenericData.Record record = bridge.encode(genRecord);
    RecordMetadata metadata = RecordMetadata.get(record);
    BaseFields fields = metadata.getBaseFields();
    assertEquals(start, fields.getTimestamp().longValue());
    assertEquals(metricName, fields.getAliasName());
    assertTrue(record.getSchema().getNamespace().endsWith(id));
  }

  public static void verifyRecordMetadataMatchesExpectedNaming(GenericRecord record) {
    RecordMetadata metadata = RecordMetadata.get(record);
    String orgId = metadata.getOrgID();
    String namespace = record.getSchema().getNamespace();
    assertTrue("Expected schema namespace (" + namespace + ") to end with " + orgId,
      namespace.endsWith(orgId));
    String fullName = record.getSchema().getFullName();
    assertTrue("Expected full name (" + fullName + " to end with canonical "
               + "name [" + metadata.getMetricCanonicalType(),
      fullName.endsWith(metadata.getMetricCanonicalType()));
  }
}
