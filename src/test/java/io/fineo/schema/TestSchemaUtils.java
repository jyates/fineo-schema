package io.fineo.schema;

import com.google.common.collect.Lists;
import io.fineo.internal.customer.metric.MetricField;
import io.fineo.internal.customer.metric.MetricMetadata;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.avro.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestSchemaUtils {
  private static final Log LOG = LogFactory.getLog(TestSchemaUtils.class);

  /**
   * Test generic {@link org.apache.avro.specific.SpecificRecordBase} serialization.
   * @throws Exception on failure
   */
  @Test
  public void testSerDe() throws Exception {
    String schema = "";
    MetricField fieldMap =
      MetricField.newBuilder().setAliases(Lists.newArrayList("fieldAlias1")).setCanonicalName(
        "fieldCName").build();
    MetricMetadata metadata = MetricMetadata.newBuilder().setAliases(null)
                                            .setCannonicalname("schemaCName")
                                            .setFieldMap(Lists.newArrayList(fieldMap))
                                            .setSchema$(schema)
                                            .build();

    verifyReadWrite(metadata);

    // set some aliases, which is a nulled-union field
    metadata = MetricMetadata.newBuilder().setAliases(Lists.newArrayList("alias1"))
      .setCannonicalname("schemaCName")
      .setFieldMap(Lists.newArrayList(fieldMap))
      .setSchema$(schema)
      .build();
    verifyReadWrite(metadata);

    // add some 'schema' in the form of sub-record
    AvroSchemaInstanceBuilder instance = new AvroSchemaInstanceBuilder();
    instance.withNamespace("ns");
    Schema s = instance.build();
    schema = s.toString();
    metadata = MetricMetadata.newBuilder().setAliases(Lists.newArrayList("alias1"))
                             .setCannonicalname("schemaCName")
                             .setFieldMap(Lists.newArrayList(fieldMap))
                             .setSchema$(schema)
                             .build();
    verifyReadWrite(metadata);
  }

  private void verifyReadWrite(MetricMetadata metadata) throws IOException {
    LOG.info("Read/Writing instance: "+metadata);
    String encoded = SchemaUtils.toString(metadata);
    MetricMetadata out = SchemaUtils.readFromString(encoded, MetricMetadata.getClassSchema());
    assertEquals(metadata, out);
  }
}
