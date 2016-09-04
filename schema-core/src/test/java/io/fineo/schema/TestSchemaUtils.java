package io.fineo.schema;

import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.MetricMetadata;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.avro.SchemaNameUtils;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class TestSchemaUtils {
  private static final Log LOG = LogFactory.getLog(TestSchemaUtils.class);

  /**
   * Test generic {@link org.apache.avro.specific.SpecificRecordBase} serialization.
   *
   * @throws Exception on failure
   */
  @Test
  public void testSerDe() throws Exception {
    String schema = "";

    Metadata baseMetadata = Metadata.newBuilder()
                                    .setCanonicalName("schemaCName")
                                    .build();
    verifyReadWrite(baseMetadata);

    MetricMetadata metricMetadata = MetricMetadata.newBuilder()
                                                  .setFields(new HashMap<>()).setMeta(baseMetadata)
                                                  .build();
    verifyReadWrite(metricMetadata);

    Metric metric = Metric.newBuilder()
                          .setMetadata(metricMetadata)
                          .setMetricSchema(schema)
                          .build();
    verifyReadWrite(metric);

    // add a field to the map
    FieldMetadata field = FieldMetadata.newBuilder()
                                       .setFieldAliases(new ArrayList<>())
                                       .setDisplayName("fieldName").build();
    verifyReadWrite(field);
    metricMetadata.getFields().put("fieldId", field);
    verifyReadWrite(metricMetadata);
    verifyReadWrite(metric);

    // add some 'schema' in the form of sub-record
    AvroSchemaInstanceBuilder instance = new AvroSchemaInstanceBuilder();
    instance.withNamespace("ns").withName("somename");
    Schema s = instance.build();
    schema = s.toString();
    metric = Metric.newBuilder(metric)
                   .setMetricSchema(schema)
                   .build();
    verifyReadWrite(metric);
  }

  private <T extends SpecificRecordBase> void verifyReadWrite(T record) throws IOException {
    LOG.info("Read/Writing instance: " + record);
    String encoded = SchemaNameUtils.toString(record);
    T out = SchemaNameUtils.readFromString(encoded, record.getSchema());
    assertEquals(record, out);
  }

  @Test
  public void testBuildNamespace() throws Exception {
    String ns = SchemaNameUtils.BASE_CUSTOMER_NAMESPACE + ".org1";
    assertEquals(ns, SchemaNameUtils.getCustomerNamespace("org1"));
    assertEquals(ns, SchemaNameUtils.getCustomerNamespace(".org1"));
    assertEquals(ns, SchemaNameUtils.getCustomerNamespace("..org1"));
    assertEquals(ns, SchemaNameUtils.getCustomerNamespace(".org1."));
    assertEquals(ns,
      SchemaNameUtils.getCustomerNamespace(".org1.."));
    assertEquals(ns,
      SchemaNameUtils.getCustomerNamespace("..org1.."));
  }

  @Test
  public void testBuildCustomerName() throws Exception {
    String org = "org1";
    String ns = SchemaNameUtils.BASE_CUSTOMER_NAMESPACE + "." + org;
    String name = ns + ".1234";
    assertEquals(name, SchemaNameUtils.getCustomerSchemaFullName("org1", "1234"));
    assertEquals(name, SchemaNameUtils.getCustomerSchemaFullName(".org1", ".1234"));
    assertEquals(name, SchemaNameUtils.getCustomerSchemaFullName("..org1", "..1234"));
    assertEquals(name, SchemaNameUtils.getCustomerSchemaFullName(".org1.", ".1234."));
    assertEquals(name,
      SchemaNameUtils.getCustomerSchemaFullName(".org1..", ".1234.."));
    assertEquals(name,
      SchemaNameUtils.getCustomerSchemaFullName("..org1..", "..1234.."));
  }
}
