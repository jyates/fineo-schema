package io.fineo.schema.store;

import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.MetricMetadata;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.MapRecord;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.Record;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.exception.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.Deflater;

import static org.junit.Assert.assertEquals;

public class SchemaTestUtils {

  public static SchemaStore getStore() {
    return new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  }

  public static SchemaStore createStoreWithBooleanFields(String orgid, String metricId,
    String... fieldNames)
    throws IOException, OldSchemaException {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    addNewOrg(store, orgid, metricId, fieldNames);
    return store;
  }

  public static SchemaBuilder.Organization addNewOrg(SchemaStore store, String orgid,
    String metricId, String... fieldNames) throws IOException, OldSchemaException {
    // create an organization and a metric type to populate the store
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.MetricBuilder schema = builder.newOrg(orgid).newMetric().withName(metricId);
    for (String field : fieldNames) {
      schema.withBoolean(field).asField();
    }
    SchemaBuilder.Organization organization = schema.build().build();
    store.createNewOrganization(organization);
    return organization;
  }

  /**
   * The fields that must be in any <i>inbound record</i> to be considered 'valid'
   */
  public static Map<String, Object> getBaseFields(String orgid, String metricId) {
    return getBaseFields(orgid, metricId, System.currentTimeMillis());
  }

  public static Map<String, Object> getBaseFields(String orgid, String metricId, long ts) {
    Map<String, Object> fields = new HashMap<>();
    fields.put(AvroSchemaProperties.ORG_ID_KEY, orgid);
    fields.put(AvroSchemaProperties.ORG_METRIC_TYPE_KEY, metricId);
    fields.put(AvroSchemaProperties.TIMESTAMP_KEY, ts);
    return fields;
  }

  public static GenericRecord writeReadRecord(SchemaStore store, String orgId, Record record)
    throws IOException {
    AvroSchemaEncoder bridge = new StoreClerk(store, orgId).getEncoderFactory().getEncoder(record);
    return SchemaTestUtils.writeReadRecord(bridge);
  }

  public static GenericRecord writeReadRecord(AvroSchemaEncoder bridge)
    throws IOException {
    GenericData.Record outRecord = bridge.encode();
    GenericRecord decoded = readWriteData(outRecord);
    assertEquals(outRecord, decoded);
    return decoded;
  }

  static GenericRecord readWriteData(GenericRecord record) throws IOException {
    byte[] data = writeRecord(record);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> reader =
      new DataFileReader<>(new SeekableByteArrayInput(data), datumReader);
    return reader.next();
  }

  public static byte[] writeRecord(GenericRecord outRecord) throws IOException {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(outRecord.getSchema());
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    fileWriter.setCodec(CodecFactory.deflateCodec(Deflater.BEST_SPEED));
    fileWriter.create(outRecord.getSchema(), out);
    fileWriter.append(outRecord);
    fileWriter.close();
    return out.toByteArray();
  }

  public static List<GenericRecord> createRandomRecord(int count) throws Exception {
    return createRandomRecord("orgId", "metricType", System.currentTimeMillis(), count);
  }

  public static List<GenericRecord> createRandomRecord(String orgId, String metricType,
    long startTs, int recordCount) throws IOException, OldSchemaException {
    int fieldCount = new Random().nextInt(10);
    return createRandomRecord(orgId, metricType, startTs, recordCount, fieldCount);
  }

  public static List<GenericRecord> createRandomRecord(String orgId, String metricType,
    long startTs, int recordCount, int fieldCount) throws IOException, OldSchemaException {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    return createRandomRecord(store, orgId, metricType, startTs, recordCount, fieldCount);
  }

  public static List<GenericRecord> createRandomRecord(SchemaStore store, String orgId,
    String metricType, long startTs, int recordCount, int fieldCount)
    throws IOException, OldSchemaException {
    // create a semi-random schema
    String[] fieldNames =
      IntStream.range(0, fieldCount)
               .mapToObj(index -> "a" + index)
               .collect(Collectors.toList())
               .toArray(new String[0]);
    SchemaTestUtils.addNewOrg(store, orgId, metricType, fieldNames);

    // create random records with the above schema
    return createRandomRecordForSchema(store, orgId, metricType, startTs, recordCount, fieldCount);
  }

  public static List<GenericRecord> createRandomRecordForSchema(SchemaStore store, String orgId,
    String metricType, long startTs, int recordCount, int fieldCount)
    throws SchemaNotFoundException {
    AvroSchemaEncoderFactory factory = new StoreClerk(store, orgId).getEncoderFactory();
    List<GenericRecord> records = new ArrayList<>(recordCount);
    for (int i = 0; i < recordCount; i++) {
      Map<String, Object> fields = SchemaTestUtils.getBaseFields(orgId, metricType, startTs + i);
      Record record = new MapRecord(fields);
      for (int j = 0; j < fieldCount; j++) {
        fields.put("a" + j, true);
      }
      records.add(factory.getEncoder(record).encode());
    }
    return records;
  }

  /**
   * Create a randomish schema using our schema generation utilities
   *
   * @return a record with a unique schema
   * @throws IOException
   */
  public static GenericRecord createRandomRecord() throws Exception {
    return createRandomRecord(1).get(0);
  }

  public static void verifyFieldType(String type, Schema.Field field) {
    Schema fieldSchema = field.schema();
    assertEquals("Schema field instance should be a union!", Schema.Type.UNION,
      fieldSchema.getType());
    assertEquals("First field schema should be a null type!", Schema.Type.NULL,
      fieldSchema.getTypes().get(0).getType());
    fieldSchema = fieldSchema.getTypes().get(1);
    List<Schema.Field> fields = fieldSchema.getFields();
    assertEquals("Wrong number of fields in FieldInstanceVisitor! Got fields " + fields, 2,
      fields.size());
    //verify the alias field
    assertEquals("displayName", fields.get(0).name());
    assertEquals("value", fields.get(1).name());
    assertEquals(type, fields.get(1).schema().getType().getName());
  }

  public static SchemaNameGenerator generateStringNames(List<String> names) {
    int[] index = new int[1];
    return () -> names.get(index[0]++);
  }

  public static Map<String, List<String>> mapFieldNames(MetricMetadata metricMetadata) {
    Map<String, FieldMetadata> schemaFieldMap = metricMetadata.getFields();
    Map<String, List<String>> aliases = new HashMap<>();
    schemaFieldMap.entrySet().stream()
                  .forEach(e -> aliases.put(e.getKey(), e.getValue().getFieldAliases()));
    return aliases;
  }

  public static Map<String, List<String>> mapAliasValueNames(OrgMetadata orgMetadata) {
    Map<String, OrgMetricMetadata> schemaFieldMap = orgMetadata.getMetrics();
    Map<String, List<String>> aliases = new HashMap<>();
    schemaFieldMap.entrySet().stream()
                  .forEach(e -> aliases.put(e.getKey(), e.getValue().getAliasValues()));
    return aliases;
  }

  public static <T> List<T> collectMapListValues(Map<?, List<T>> map) {
    return map.values().stream()
              .sequential()
              .flatMap(list -> list.stream())
              .collect(Collectors.toList());
  }
}
