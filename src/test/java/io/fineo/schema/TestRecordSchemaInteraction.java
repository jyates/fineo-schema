package io.fineo.schema;

import io.fineo.internal.customer.Metadata;
import io.fineo.schema.avro.AvroSchemaBridge;
import io.fineo.schema.avro.SchemaNameGenerator;
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
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test the interaction we go through with the schema store when we get a record and need to
 * convert it to an avro-serialized form (and reading it back out).
 */
public class TestRecordSchemaInteraction {

  @Test
  public void testLoadSchema() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    // create an organization and a metric type to populate the store
    SchemaBuilder builder = new SchemaBuilder(new SchemaNameGenerator());
    String id = "123d43";
    String field = "bField";
    String metricName = "newschema";
    SchemaBuilder.OrganizationBuilder metadata = builder.newOrg(id)
                                                        .newSchema().withName(metricName)
                                                        .withBoolean(field).asField()
                                                        .build();
    SchemaBuilder.Organization organization = metadata.build();
    Metadata meta = organization.getMetadata();
    store.createNewOrganization(organization);

    // create a simple record with a field the same name of the metric type we created
    Map<String, Object> fields = new HashMap<>();
    fields.put(SchemaBuilder.ORG_ID_KEY, id);
    Map<String, List<String>> metrics = meta.getMetricTypes().getCanonicalNamesToAliases();
    fields.put(SchemaBuilder.ORG_METRIC_TYPE_KEY, metricName);
    fields.put(field, true);
    String unknown = "unknownFieldName";
    fields.put(unknown, "1231");
    Record record = new MapRecord(fields);

    //create a bridge between the record and the avro type
    String orgid = record.getStringByField(SchemaBuilder.ORG_ID_KEY);
    Metadata orgMetadata = store.getSchemaTypes(orgid);
    String type = record.getStringByField(SchemaBuilder.ORG_METRIC_TYPE_KEY);
    AvroSchemaBridge bridge = AvroSchemaBridge.create(orgMetadata,store, type);
    assertNotNull("didn't find a matching metric name for alias: " + type + "!", bridge);
    GenericData.Record outRecord = bridge.encode(record);

    // write it out
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(outRecord.getSchema());
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    fileWriter.setCodec(CodecFactory.deflateCodec(Deflater.BEST_SPEED));
    fileWriter.create(outRecord.getSchema(), out);
    fileWriter.append(outRecord);
    fileWriter.close();

    // read the record back
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> reader =
      new DataFileReader<>(new SeekableByteArrayInput(out.toByteArray()), datumReader);
    assertEquals(outRecord, reader.next());
  }
}
