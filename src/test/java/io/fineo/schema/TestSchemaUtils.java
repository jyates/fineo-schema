package io.fineo.schema;

import com.google.common.collect.Lists;
import io.fineo.internal.customer.FieldNameMap;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.avro.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    Map<String, List<String>> fields = new HashMap<>(1);
    fields.put("fieldCName", Lists.newArrayList(" fieldAlias1"));
    FieldNameMap map = FieldNameMap.newBuilder().setCanonicalNamesToAliases(fields).build();
    // read/write the map to ensure we support more than 1 type
    verifyReadWrite(map);

    Metadata metricMetadata = Metadata.newBuilder()
                                      .setCanonicalName("schemaCName")
                                      .setMetricTypes(FieldNameMap
                                        .newBuilder()
                                        .setCanonicalNamesToAliases(null)
                                        .build())
                                      .build();
    verifyReadWrite(metricMetadata);

    Metric metric = Metric.newBuilder()
                          .setMetadata(metricMetadata)
                          .setMetricSchema(schema)
                          .build();
    verifyReadWrite(metric);

    // set some aliases, which is a nulled-union field
    metric = Metric
      .newBuilder()
      .setMetadata(Metadata
        .newBuilder()
        .setMetricTypes(map)
        .setCanonicalName("schemaCName")
        .build())
      .setMetricSchema(schema)
      .build();
    verifyReadWrite(metric);

    // add some 'schema' in the form of sub-record
    AvroSchemaInstanceBuilder instance = new AvroSchemaInstanceBuilder(new SchemaNameGenerator());
    instance.withNamespace("ns");
    Schema s = instance.build();
    schema = s.toString();
    metric = Metric
      .newBuilder()
      .setMetadata(Metadata
        .newBuilder()
        .setMetricTypes(map)
        .setCanonicalName("schemaCName")
        .build())
      .setMetricSchema(schema)
      .build();
    verifyReadWrite(metric);
  }

  private <T extends SpecificRecordBase> void verifyReadWrite(T record) throws IOException {
    LOG.info("Read/Writing instance: " + record);
    String encoded = SchemaUtils.toString(record);
    T out = SchemaUtils.readFromString(encoded, record.getSchema());
    assertEquals(record, out);
  }

  @Test
  public void testBuildNamespace() throws Exception {
    String ns = SchemaUtils.BASE_CUSTOMER_NAMESPACE + ".org1";
    assertEquals(ns, SchemaUtils.getCustomerNamespace("org1"));
    assertEquals(ns, SchemaUtils.getCustomerNamespace(".org1"));
    assertEquals(ns, SchemaUtils.getCustomerNamespace("..org1"));
    assertEquals(ns, SchemaUtils.getCustomerNamespace(".org1."));
    assertEquals(ns,
      SchemaUtils.getCustomerNamespace(".org1.."));
    assertEquals(ns,
      SchemaUtils.getCustomerNamespace("..org1.."));
  }

  @Test
  public void testBuildCustomerName() throws Exception {
    String org = "org1";
    String ns = SchemaUtils.BASE_CUSTOMER_NAMESPACE + "." + org;
    String name = ns + ".1234";
    assertEquals(name, SchemaUtils.getCustomerSchemaFullName("org1", "1234"));
    assertEquals(name, SchemaUtils.getCustomerSchemaFullName(".org1", ".1234"));
    assertEquals(name, SchemaUtils.getCustomerSchemaFullName("..org1", "..1234"));
    assertEquals(name, SchemaUtils.getCustomerSchemaFullName(".org1.", ".1234."));
    assertEquals(name,
      SchemaUtils.getCustomerSchemaFullName(".org1..", ".1234.."));
    assertEquals(name,
      SchemaUtils.getCustomerSchemaFullName("..org1..", "..1234.."));
  }
}
