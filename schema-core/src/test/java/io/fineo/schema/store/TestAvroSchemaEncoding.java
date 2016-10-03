package io.fineo.schema.store;

import io.fineo.internal.customer.BaseFields;
import io.fineo.schema.MapRecord;
import io.fineo.schema.Pair;
import io.fineo.schema.Record;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.timestamp.MultiPatternTimestampParser;
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

  private final String fixedTsString = "Tue, 06 Sep 2016 19:00:46 GMT";
  private final long fixedTs = 1473188446000l;

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

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.ORG_ID_KEY, org);
    map.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    map.put(AvroSchemaProperties.TIMESTAMP_KEY, 10);
    map.put(f1, 1.0);
    map.put(f2, "true");
    map.put(f3, 1);
    map.put(f4, 1);
    map.put(f5, "1");
    MapRecord mapRecord = new MapRecord(map);

    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, TimeZone.getDefault().toZoneId());
    StoreClerk clerk = new StoreClerk(store, org);
    AvroSchemaEncoder encoder = clerk.getEncoderFactory().getEncoder(mapRecord);
    encoder.setClockForTesting(clock);
    GenericRecord out = encoder.encode();
    BaseFields base = (BaseFields) out.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    assertEquals((Long) now.toEpochMilli(), base.getWriteTime());
    assertEquals(now.toEpochMilli(), base.get(AvroSchemaProperties.WRITE_TIME_KEY));
    assertEquals((Long) 10l, base.getTimestamp());
    assertEquals(newHashMap(), base.getUnknownFields());
    assertEquals(metric, base.getAliasName());
    assertEquals(metric, base.get(AvroSchemaProperties.METRIC_ORIGINAL_FIELD_ALIAS));

    // check the field values
    Record translated = new AvroRecordTranslator(out, store).getTranslatedRecord();
    assertEquals(map.get(f1), translated.getDoubleByFieldName(f1));
    assertEquals(map.get(f2), translated.getStringByField(f2));
    assertEquals(Long.valueOf(map.get(f3).toString()), translated.getLongByFieldName(f3));
    assertEquals(Float.valueOf(map.get(f4).toString()), translated.getFloatByFieldName(f4));
    assertEquals(map.get(f5), translated.getStringByField(f5));
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

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.ORG_ID_KEY, org);
    map.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    map.put(AvroSchemaProperties.TIMESTAMP_KEY, 10);
    map.put(f1, 1);
    map.put(f2, "true");
    map.put(f3, 1);
    map.put(f4, 1);
    map.put(f5, "1");
    MapRecord mapRecord = new MapRecord(map);

    StoreClerk clerk = new StoreClerk(store, org);
    AvroSchemaEncoder encoder = clerk.getEncoderFactory().getEncoder(mapRecord);
    GenericData.Record record = encoder.encode();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
    fileWriter.create(record.getSchema(), out);
    fileWriter.append(record);
    fileWriter.close();
    out.flush();
  }

  @Test
  public void testChangeTimestampAliasStillWorksWithParsing() throws Exception {
    SchemaStore store = getStore();
    StoreManager storeManager = new StoreManager(store);
    String org = "org", metric = "m1", f = "f1", tsAlias = "alias";
    TestSchemaManager.commitSimpleType(storeManager, org, metric, of(), p(f, "INTEGER"));
    storeManager.updateOrg(org)
                .updateMetric(metric).addFieldAlias(AvroSchemaProperties.TIMESTAMP_KEY, tsAlias)
                .build()
                .commit();

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.ORG_ID_KEY, org);
    map.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    map.put(tsAlias, fixedTs);
    map.put(f, 1);

    GenericRecord out = writeRecordAndValidateAtNow(store, org, map);
    BaseFields base = (BaseFields) out.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    assertEquals((Long) fixedTs, base.getTimestamp());
    assertEquals(newHashMap(), base.getUnknownFields());
    assertEquals(metric, base.getAliasName());
    assertEquals(metric, base.get(AvroSchemaProperties.METRIC_ORIGINAL_FIELD_ALIAS));

    // check the field values
    Record translated = new AvroRecordTranslator(out, store).getTranslatedRecord();
    assertEquals(map.get(f), translated.getIntegerByField(f));
  }

  @Test
  public void testCustomTimestampParsingFormat() throws Exception {
    SchemaStore store = getStore();
    StoreManager storeManager = new StoreManager(store);
    String org = "org", metric = "m1", f = "f1";
    TestSchemaManager.commitSimpleType(storeManager, org, metric, of(), p(f, "INTEGER"));
    storeManager.updateOrg(org)
                .withTimestampFormat(
                  MultiPatternTimestampParser.TimeFormats.RFC_1123_DATE_TIME.name())
                .commit();

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.ORG_ID_KEY, org);
    map.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    map.put(AvroSchemaProperties.TIMESTAMP_KEY, fixedTsString);
    map.put(f, 1);

    GenericRecord out = writeRecordAndValidateAtNow(store, org, map);
    BaseFields base = (BaseFields) out.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    assertEquals((Long) fixedTs, base.getTimestamp());
    assertEquals(newHashMap(), base.getUnknownFields());
    assertEquals(metric, base.getAliasName());
    assertEquals(metric, base.get(AvroSchemaProperties.METRIC_ORIGINAL_FIELD_ALIAS));

    // check the field values
    Record translated = new AvroRecordTranslator(out, store).getTranslatedRecord();
    assertEquals(map.get(f), translated.getIntegerByField(f));
  }

  @Test
  public void testCustomTimestampParsingAndAlias() throws Exception {
    SchemaStore store = getStore();
    StoreManager storeManager = new StoreManager(store);
    String org = "org", metric = "m1", f = "f1", tsAlias = "alias";
    TestSchemaManager.commitSimpleType(storeManager, org, metric, of(), p(f, "INTEGER"));
    storeManager.updateOrg(org)
                .withTimestampFormat(MultiPatternTimestampParser.TimeFormats.RFC_1123_DATE_TIME
                  .name())
                .updateMetric(metric).addFieldAlias(AvroSchemaProperties.TIMESTAMP_KEY, tsAlias)
                .build()
                .commit();

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.ORG_ID_KEY, org);
    map.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metric);
    map.put(tsAlias, fixedTsString);
    map.put(f, 1);

    GenericRecord out = writeRecordAndValidateAtNow(store, org, map);
    BaseFields base = (BaseFields) out.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    assertEquals((Long) fixedTs, base.getTimestamp());
    assertEquals(newHashMap(), base.getUnknownFields());
    assertEquals(metric, base.getAliasName());
    assertEquals(metric, base.get(AvroSchemaProperties.METRIC_ORIGINAL_FIELD_ALIAS));

    // check the field values
    Record translated = new AvroRecordTranslator(out, store).getTranslatedRecord();
    assertEquals(map.get(f), translated.getIntegerByField(f));
  }

  private GenericRecord writeRecordAndValidateAtNow(SchemaStore store, String org,
    Map<String, Object> map) throws SchemaNotFoundException {
    MapRecord mapRecord = new MapRecord(map);
    Instant now = Instant.now();
    Clock clock = Clock.fixed(now, TimeZone.getDefault().toZoneId());
    StoreClerk clerk = new StoreClerk(store, org);
    AvroSchemaEncoder encoder = clerk.getEncoderFactory().getEncoder(mapRecord);
    encoder.setClockForTesting(clock);
    GenericRecord out = encoder.encode();
    BaseFields base = (BaseFields) out.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    assertEquals((Long) now.toEpochMilli(), base.getWriteTime());
    assertEquals(now.toEpochMilli(), base.get(AvroSchemaProperties.WRITE_TIME_KEY));
    return out;
  }

  private <T, V> Pair<T, V> p(T t, V v) {
    return new Pair<>(t, v);
  }

  private SchemaStore getStore() {
    return new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  }
}
