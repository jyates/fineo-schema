package io.fineo.schema.store;

import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Pair;
import io.fineo.schema.Record;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;

public class TestAvroSchemaEncoding {

  @Test
  public void testBasicEncoding() throws Exception {
    SchemaStore store = getStore();
    StoreManager storeManager = new StoreManager(store);
    String org = "org", metric = "m1",
      f1 = "f1", f2 = "f2", f3 = "f3", f4 = "f4", f5 = "f5";
    TestSchemaManager.commitSimpleType(storeManager, org, metric, of(),
      p(f1, "DOUBLE"),
      p(f2, "BOOLEAN"),
      p(f3, "LONG"),
      p(f4, "FLOAT"),
      p(f5, "INTEGER"));

    Map<String, Object> mapRecord = new HashMap<>();
    mapRecord.put(AvroSchemaProperties.ORG_ID_KEY, org);
    mapRecord.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    mapRecord.put(AvroSchemaProperties.TIMESTAMP_KEY, 10);
    mapRecord.put(f1, 1.0);
    mapRecord.put(f2, "true");
    mapRecord.put(f3, 1);
    mapRecord.put(f4, 1);
    mapRecord.put(f5, "1");

    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, TimeZone.getDefault().toZoneId());
    OrgMetadata orgMetadata = store.getOrgMetadata(org);
    String metricCName = store.getMetricCNameFromAlias(orgMetadata, metric);
    OrgMetricMetadata metadata = store.getOrgMetricMetadataForAliasMetricName(orgMetadata, metric);
    Metric storedMetric = store.getMetricMetadata(org, metricCName);
    AvroSchemaEncoder encoder = new AvroSchemaEncoder(org, metadata, storedMetric, clock);
    GenericRecord out = encoder.encode(new MapRecord(mapRecord));
    BaseFields base = (BaseFields) out.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    assertEquals((Long)now.toEpochMilli(), base.getWriteTime());
    assertEquals((Long)10l, base.getTimestamp());
    assertEquals(newHashMap(), base.getUnknownFields());
    assertEquals(metric, base.getAliasName());

    // check the field values
    Record translated = new AvroRecordTranslator(out, store).getTranslatedRecord();
    assertEquals(mapRecord.get(f1), translated.getDoubleByFieldName(f1));
    assertEquals(mapRecord.get(f2), translated.getStringByField(f2));
    assertEquals(Long.valueOf(mapRecord.get(f3).toString()), translated.getLongByFieldName(f3));
    assertEquals(Float.valueOf(mapRecord.get(f4).toString()), translated.getFloatByFieldName(f4));
    assertEquals(mapRecord.get(f5), translated.getStringByField(f5));
  }
  
  @Test
  public void testWrongTypedRecordField() throws Exception {
    SchemaStore store = getStore();
    StoreManager storeManager = new StoreManager(store);
    String org = "org", metric = "m1",
      f1 = "f1", f2 = "f2", f3 = "f3", f4 = "f4", f5 = "f5";
    TestSchemaManager.commitSimpleType(storeManager, org, metric, of(),
      p(f1, "DOUBLE"),
      p(f2, "BOOLEAN"),
      p(f3, "LONG"),
      p(f4, "FLOAT"),
      p(f5, "INTEGER"));

    Map<String, Object> mapRecord = new HashMap<>();
    mapRecord.put(AvroSchemaProperties.ORG_ID_KEY, org);
    mapRecord.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    mapRecord.put(AvroSchemaProperties.TIMESTAMP_KEY, 10);
    mapRecord.put(f1, 1);
    mapRecord.put(f2, "true");
    mapRecord.put(f3, 1);
    mapRecord.put(f4, 1);
    mapRecord.put(f5, "1");

    AvroSchemaManager manager = new AvroSchemaManager(store, org);
    AvroSchemaEncoder encoder = manager.encode(metric);
    GenericData.Record record = encoder.encode(new MapRecord(mapRecord));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
    fileWriter.create(record.getSchema(), out);
    fileWriter.append(record);
    fileWriter.close();
    out.flush();
  }

  private <T, V> Pair<T, V> p(T t, V v) {
    return new Pair<>(t, v);
  }

  private SchemaStore getStore() {
    return new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  }
}
