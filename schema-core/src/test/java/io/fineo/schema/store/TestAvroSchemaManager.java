package io.fineo.schema.store;


import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Record;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.avro.SchemaNameUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Encoding/Decoding of metrics is consistent using the manager
 */
public class TestAvroSchemaManager {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSimpleSchemaMapping() throws Exception {
    // start with a simple record that uses customer defined fields
    Map<String, Object> record = new HashMap<>();
    String orgId = "orgid";
    record.put(AvroSchemaEncoder.ORG_ID_KEY, orgId);
    String orgMetric = "org-visible-metric-name";
    record.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, orgMetric);
    record.put(AvroSchemaEncoder.TIMESTAMP_KEY, 10l);
    String orgFieldName = "org-aliased-key";
    record.put(orgFieldName, "true");

    // create a schema for the record
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaTestUtils.addNewOrg(store, orgId, orgMetric, orgFieldName);

    // Ensure the schema mapping happens as we expect
    OrgMetadata orgMetadata = store.getOrgMetadata(orgId);
    assertEquals("Canonical org name doesn't match client specified org name", orgId,
      orgMetadata.getMetadata().getCanonicalName());
    Metric metric = store.getMetricMetadataFromAlias(orgMetadata, orgMetric);
    // means we found the metric based on the alias
    assertNotNull("No metric available for metric", metric);
    assertNotEquals(orgMetric, metric.getMetadata().getMeta().getCanonicalName());
    Schema schema = metric.getSchema();
    assertFalse("Alias field name present in schema!",
      schema.getFields().stream().map(field -> field.name())
            .anyMatch(name -> name.equals(orgFieldName)));

    // encode the record and ensure that we can read it as we expect (with canonical fields)
    AvroSchemaManager manager = new AvroSchemaManager(store, orgId);
    AvroSchemaEncoder encoder = manager.encode(orgMetric);
    MapRecord mapRecord = new MapRecord(record);
    GenericRecord avro = encoder.encode(mapRecord);

    // ensure that we encoded it correctly
    Assert.assertEquals(SchemaNameUtils.getOrgId(avro.getSchema().getNamespace()), orgId);

    AvroRecordTranslator translator = manager.translator(avro);
    RecordMetadata metadata = translator.getMetadata();
    assertEquals("Decoded metadata orgID doesn't match stored", metadata.getOrgID(), orgId);
    assertEquals("Schema metric type name doesn't match metric metadata canonical name",
      metric.getMetadata().getMeta().getCanonicalName(), metadata.getMetricCanonicalType());
    assertNull("Record has a field with alias name!", avro.get(orgFieldName));
    // but this record has translated fields!
    Record translated = translator.getTranslatedRecord();
    assertEquals(mapRecord.getBooleanByField(orgFieldName), translated.getField(orgFieldName));
  }

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
    AvroSchemaEncoder bridge = SchemaTestUtils.getBridgeForSchema(store, record);
    SchemaTestUtils.writeReadRecord(bridge, record);
  }

  @Test
  public void testCreateWithBadRecords() throws Exception {
    SchemaStore store = Mockito.mock(SchemaStore.class);
    Map<String, Object> content = new HashMap<>();
    MapRecord record = new MapRecord(content);
    verifyIllegalCreate(store, record, "when no metric or orgid");

    content.put(AvroSchemaEncoder.ORG_ID_KEY, "orgid");
    verifyIllegalCreate(store, record, "when no orgid, but metricId present");

    content.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, "metricid");
    content.remove(AvroSchemaEncoder.ORG_ID_KEY);
    verifyIllegalCreate(store, record, "when no metricId, but orgId present");

    content.put(AvroSchemaEncoder.ORG_ID_KEY, "orgid");
    verifyIllegalCreate(store, record,
      "when no metadata received from store, even when record had all necessary fields");

    Metadata base = Mockito.mock(Metadata.class);
    Mockito.when(base.getCanonicalName()).thenReturn("orgid");
    OrgMetadata meta = Mockito.mock(OrgMetadata.class);
    Mockito.when(store.getOrgMetadata("orgid")).thenReturn(meta);
    Mockito.when(meta.getMetadata()).thenReturn(base);
    verifyIllegalCreate(store, record, "when org id exists, but metric type not found");
    Mockito.verify(meta).getMetadata();
    Mockito.verify(base).getCanonicalName();
    Mockito.verify(store, Mockito.times(2)).getOrgMetadata("orgid");
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateExistingOrg() throws Exception {
    Map<String, Object> record = new HashMap<>();
    String orgId = "orgid";
    record.put(AvroSchemaEncoder.ORG_ID_KEY, orgId);
    String orgMetric = "org-visible-metric-name";
    record.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, orgMetric);
    record.put(AvroSchemaEncoder.TIMESTAMP_KEY, 10l);
    String orgFieldName = "org-aliased-key";
    record.put(orgFieldName, "true");

    // create a schema for the record
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaTestUtils.addNewOrg(store, orgId, orgMetric, orgFieldName);
    SchemaTestUtils.addNewOrg(store, orgId, orgMetric, orgFieldName);
  }

  @Test
  public void testIntegerTimestamp() throws Exception {
    // start with a simple record that uses customer defined fields
    Map<String, Object> record = new HashMap<>();
    String orgId = "orgid";
    record.put(AvroSchemaEncoder.ORG_ID_KEY, orgId);
    String orgMetric = "org-visible-metric-name";
    record.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, orgMetric);
    record.put(AvroSchemaEncoder.TIMESTAMP_KEY, 10);
    String orgFieldName = "org-aliased-key";
    record.put(orgFieldName, "true");

    // create a schema for the record
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaTestUtils.addNewOrg(store, orgId, orgMetric, orgFieldName);

    AvroSchemaManager manager = new AvroSchemaManager(store, orgId);
    AvroSchemaEncoder encoder = manager.encode(orgMetric);
    GenericRecord avro = encoder.encode(new MapRecord(record));
    AvroRecordTranslator translator = manager.translator(avro);
    RecordMetadata metadata = translator.getMetadata();
    assertEquals(10L, (long) metadata.getBaseFields().getTimestamp());
  }

  @Test
  public void testInvalidFieldNames() throws Exception {
    Map<String, Object> record = new HashMap<>();
    String orgId = "orgid";
    record.put(AvroSchemaEncoder.ORG_ID_KEY, orgId);
    String orgMetric = "org-visible-metric-name";
    record.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, orgMetric);
    record.put(AvroSchemaEncoder.TIMESTAMP_KEY, 10);
    String orgFieldName = "org-aliased-key";
    record.put(orgFieldName, "true");
    // unknown fields that have invalid names
    for (String bad : TestSchemaManager.BAD_FIELD_NAMES) {
      record.put(bad, true);
    }

    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaTestUtils.addNewOrg(store, orgId, orgMetric, orgFieldName);
    AvroSchemaManager manager = new AvroSchemaManager(store, orgId);
    AvroSchemaEncoder encoder = manager.encode(orgMetric);

    thrown.expect(RuntimeException.class);
    thrown.expect(TestSchemaManager.expectFailedFields(TestSchemaManager.BAD_FIELD_NAMES));
    encoder.encode(new MapRecord(record));
  }

  private void verifyIllegalCreate(SchemaStore store, Record record, String when) {
    try {
      AvroSchemaEncoder.create(store, record);
      fail("Didn't throw illegal argument exception " + when);
    } catch (IllegalArgumentException | NullPointerException e) {
      //expected
    }
  }
}
