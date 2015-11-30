package io.fineo.schema;

import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.SchemaNameGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    SchemaBuilder.OrganizationBuilder metadata = builder.newOrg(id);
    String field = "bField";
    metadata.newSchema().withName("newschema").withBoolean(field).asField().build();
    SchemaBuilder.Organization organization = metadata.build();
    Metadata meta = organization.getMetadata();
    store.createNewOrganization(organization);

    // create a simple record with a field the same name of the metric type we created
    Map<String, Object> fields = new HashMap<>();
    fields.put(SchemaBuilder.ORG_ID_KEY, id);
    Map<String, List<String>> metrics = meta.getMetricTypes().getCanonicalNamesToAliases();
    fields.put(SchemaBuilder.ORG_METRIC_TYPE_KEY, metrics.keySet().iterator().next());
    fields.put(field, "true");
    String unknown = "unknownFieldName";
    fields.put(unknown, "1231");
    Record record = new MapRecord(fields);

    // try to understand the schema to which the record should conform
    String orgid = record.getStringByField(SchemaBuilder.ORG_ID_KEY);
    // load the schema for the org, which is really just a bunch of names of possible schemas
    Metadata orgMetadata = store.getSchemaTypes(orgid);
    String type = record.getStringByField(SchemaBuilder.ORG_METRIC_TYPE_KEY);
    // for each schema name (metric type) load the actual metric information
    Metric metric = null;
    for (Map.Entry<String, List<String>> metricNameAlias : metrics.entrySet()) {
      // first alias set that matches
      if(metricNameAlias.getValue().contains(type)){
        metric = store.getMetricMetadata(orgMetadata.getCanonicalName(),
          String.valueOf(metricNameAlias.getKey()));
        break;
      }
    }

    assertNotNull("didn't find a matching metric name!", metric);

    Schema.Parser parser = new Schema.Parser();
    parser.parse(String.valueOf(metric.getSchema()));
    Schema schema = parser.getTypes().get(metric.getSchema());

    // turn it into an avro record
    GenericData.Record avroRecord = new GenericData.Record(schema);
    // ignoring org and type, write the fields into the record
    for (Map.Entry<String, Object> fieldEntry : record.getFields()) {
      String key = fieldEntry.getKey();
      if (key.equals(SchemaBuilder.ORG_ID_KEY) || key.equals(SchemaBuilder.ORG_METRIC_TYPE_KEY)) {
        continue;
      } else if (schema.getField(key) == null) {
        ((Map<String, String>) avroRecord.get(SchemaBuilder.UNKNWON_KEYS))
          .put(key, String.valueOf(fieldEntry.getValue()));
      } else {
        avroRecord.put(key, fieldEntry.getValue());
      }
    }

    // write it out
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder enc = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(avroRecord, enc);

    out.close();
    // read the record back
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
    Decoder dec = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord read = reader.read(null, dec);
    assertEquals(avroRecord, read);
  }
}
