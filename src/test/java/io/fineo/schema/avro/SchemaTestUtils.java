package io.fineo.schema.avro;

import io.fineo.schema.OldSchemaException;
import io.fineo.schema.Record;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
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
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class SchemaTestUtils {

  public static SchemaStore createStoreWithBooleanFields(String orgid, String metricId,
    String... fieldNames)
    throws IOException, OldSchemaException {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    addNewOrg(store, orgid, metricId, fieldNames);
    return store;
  }

  public static void addNewOrg(SchemaStore store, String orgid, String metricId,
    String... fieldNames) throws IOException, OldSchemaException {
    // create an organization and a metric type to populate the store
    SchemaBuilder builder = new SchemaBuilder(new SchemaNameGenerator());
    SchemaBuilder.MetadataBuilder schema = builder.newOrg(orgid).newSchema().withName(metricId);
    for (String field : fieldNames) {
      schema.withBoolean(field).asField();
    }
    SchemaBuilder.Organization organization = schema.build().build();
    store.createNewOrganization(organization);
  }

  public static Map<String, Object> getBaseFields(String orgid, String metricid) {
    Map<String, Object> fields = new HashMap<>();
    fields.put(SchemaBuilder.ORG_ID_KEY, orgid);
    fields.put(SchemaBuilder.ORG_METRIC_TYPE_KEY, metricid);
    return fields;
  }

  public static AvroSchemaBridge getBridgeForSchema(SchemaStore store, Record record) {
    AvroSchemaBridge bridge = AvroSchemaBridge.create(store, record);
    assertNotNull("didn't find a matching metric name for record: "+record, bridge);
    return bridge;
  }

  public static void readWriteRecord(AvroSchemaBridge bridge, Record record) throws IOException {
    GenericData.Record outRecord = bridge.encode(record);
    byte[] data = writeRecord(outRecord);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> reader =
      new DataFileReader<>(new SeekableByteArrayInput(data), datumReader);
    assertEquals(outRecord, reader.next());
  }

  public static byte[] writeRecord(GenericData.Record outRecord) throws IOException {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(outRecord.getSchema());
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    fileWriter.setCodec(CodecFactory.deflateCodec(Deflater.BEST_SPEED));
    fileWriter.create(outRecord.getSchema(), out);
    fileWriter.append(outRecord);
    fileWriter.close();
    return out.toByteArray();
  }
}
