package io.fineo.schema;

import com.google.common.collect.Lists;
import io.fineo.internal.customer.metric.MetricField;
import io.fineo.internal.customer.metric.MetricMetadata;
import io.fineo.internal.customer.metric.OrganizationMetadata;
import io.fineo.schema.avro.SchemaNameGenerator;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test that we build logical organization, metric type and field schemas as expected.
 */
public class TestSchemaBuilder {

  @Test
  public void testNewOrg() throws Exception{
    List<String> names = Lists.newArrayList("n1", "n2", "n3");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    MetricMetadata record = buildMetric(builder);
    String orgId = "12d4";
    OrganizationMetadata metadata = builder.newOrg(orgId).addMetadata(record).build();
    assertEquals(orgId, metadata.getOrgId());
    assertEquals(1, metadata.getFieldTypes().size());
    CharSequence field = metadata.getFieldTypes().get(0);
    assertEquals(names.get(2), field);
  }

  @Test
  public void testUpdateOrg() throws Exception{
    List<String> names = Lists.newArrayList("n1", "n2", "n3");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    MetricMetadata record = buildMetric(builder);
    String orgId = "12d4";
    OrganizationMetadata metadata = builder.newOrg(orgId).addMetadata(record).build();

    // setup a new builder
    names = Lists.newArrayList("r1", "r2", "r3");
    gen = setupMockNameGenerator(names);
    builder = new SchemaBuilder(gen);
    MetricMetadata record2 = buildMetric(builder);
    metadata = builder.updateOrg(metadata).addMetadata(record2).build();
    assertEquals(orgId, metadata.getOrgId());
    assertEquals(2, metadata.getFieldTypes().size());
    assertEquals(Lists.newArrayList("n3", "r3"), metadata.getFieldTypes());
  }

  @Test
  public void testNewMetric() throws Exception {
    List<String> names = Lists.newArrayList("n1", "n2", "n3");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    MetricMetadata record = buildMetric(builder);

    // verify the metric metadata itself
    assertEquals(names.get(2), record.getCannonicalname());
    assertEquals(0, record.getAliases().size());

    // verify each field
    List<MetricField> fields = record.getFields();
    assertEquals(2, fields.size());
    MetricField field = fields.get(0);
    assertEquals(names.get(0), field.getCannonicalname());
    assertEquals(
      "Aliases are other names for the field whereas the field name is the 'most recent' (last) "
      + "field in the alias array",
      Lists.newArrayList("aliasname", "bField"), field.getAliases());
    assertEquals("boolean", field.getType());

    field = fields.get(1);
    assertEquals(names.get(1), field.getCannonicalname());
    assertEquals(Lists.newArrayList("sField"), field.getAliases());
    assertEquals("string", field.getType());
  }

  private SchemaNameGenerator setupMockNameGenerator(List<String> names){
    SchemaNameGenerator gen = Mockito.mock(SchemaNameGenerator.class);
    int[] index = new int[1];
    Mockito.when(gen.generateSchemaName()).then(innvocation -> {
      int i = index[0]++;
      return names.get(i);
    });
    return gen;
  }

  private MetricMetadata buildMetric(SchemaBuilder builder) {
    return builder.newSchema()
           .withBoolean("bField").withAlias("aliasname").asField()
           .withString("sField").asField()
           .build();
  }
}
