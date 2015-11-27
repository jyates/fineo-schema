package io.fineo.schema;

import com.google.common.collect.Lists;
import io.fineo.internal.customer.metric.MetricField;
import io.fineo.internal.customer.metric.MetricMetadata;
import io.fineo.internal.customer.metric.OrganizationMetadata;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.avro.SchemaUtils;
import org.apache.avro.Schema;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test that we build logical organization, metric type and field schemas as expected.
 */
public class TestSchemaBuilder {

  private static final String ORG_ID = "orgId";
  public static final String NEW_SCHEMA_DISPLAY_NAME = "newschema";

  @Test
  public void testNewOrg() throws Exception {
    List<String> names = Lists.newArrayList("n1", "n2", "n3");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithMetric(builder).build();
    OrganizationMetadata metadata = organization.getMetadata();
    assertEquals(ORG_ID, metadata.getOrgId());
    assertEquals(1, metadata.getFieldTypes().size());
    CharSequence field = metadata.getFieldTypes().get(0);
    assertEquals(names.get(2), field);
    assertEquals(1, organization.getSchemas().size());

    // read the schema out and ensure we get the fields we added
    MetricMetadata metricSchema = organization.getSchemas().get(0);
    Schema schema =
      SchemaUtils.parseSchema(metricSchema.getSchema$(), metricSchema.getCannonicalname());
    // spot check because we verify the fields elsewhere
    List<Schema.Field> fields = schema.getFields();
    int[] contains = new int[1];
    Map<String, String> fieldNameTypes = new HashMap<>();
    fieldNameTypes.put("bField", "boolean");
    fieldNameTypes.put("sField", "string");
    fields.stream().forEach(schemaField -> {
      String type = fieldNameTypes.get(schemaField.name());
      if (type != null) {
        assertEquals(type, schemaField.getProp("type"));
      }
      contains[0]++;
    });
  }

  @Test
  public void testUpdateOrg() throws Exception {
    List<String> names = Lists.newArrayList("n1", "n2", "n3");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithMetric(builder).build();


    names = Lists.newArrayList("r1", "r2", "r3");
    gen = setupMockNameGenerator(names);
    builder = new SchemaBuilder(gen);
    // should be able to add a new schema with the same 'name' (but different canonical name)
    organization = addMetric(builder.updateOrg(organization)).build();
    OrganizationMetadata metadata = organization.getMetadata();
    assertEquals(ORG_ID, metadata.getOrgId());
    assertEquals(2, metadata.getFieldTypes().size());
    assertEquals(Lists.newArrayList("n3", "r3"), metadata.getFieldTypes());
  }

  @Test
  public void testNewMetric() throws Exception {
    List<String> names = Lists.newArrayList("n1", "n2", "n3");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization org = newOrgBuilderWithMetric(builder).build();
    assertEquals(1, org.getSchemas().size());
    MetricMetadata schema = org.getSchemas().get(0);
    // verify the metric metadata itself
    assertEquals(names.get(2), schema.getCannonicalname());
    assertEquals(1, schema.getAliases().size());
    assertEquals(NEW_SCHEMA_DISPLAY_NAME, schema.getAliases().get(0));

    // verify each field
    List<MetricField> fields = schema.getFieldMap();
    assertEquals("Expected 3 base fields and 2 added", 5, fields.size());
    MetricField field = fields.get(1);
    assertEquals(names.get(1), field.getCanonicalName());
    assertEquals(
      "Aliases are other names for the field whereas the field name is the 'most recent' (last) "
      + "field in the alias array",
      Lists.newArrayList("sField"), field.getAliases());

    field = fields.get(0);
    assertEquals(names.get(0), field.getCanonicalName());
    assertEquals(Lists.newArrayList("aliasname", "bField"), field.getAliases());

    // unwrap the schema and check the field types
    Schema.Parser parser = new Schema.Parser();
    parser.parse(String.valueOf(schema.getSchema$()));
    Schema recordSchema = parser.getTypes().get("org." + schema.getCannonicalname());
    assertEquals(5, recordSchema.getFields().size());
    List<Schema.Field> recordFields = recordSchema.getFields();
    assertEquals("sField", recordFields.get(0).name());
  }

  @Test
  @Ignore("not yet implemented")
  public void testUpdateMetric() throws Exception{
    fail();
  }

  private SchemaNameGenerator setupMockNameGenerator(List<String> names) {
    SchemaNameGenerator gen = Mockito.mock(SchemaNameGenerator.class);
    int[] index = new int[1];
    Mockito.when(gen.generateSchemaName()).then(innvocation -> {
      int i = index[0]++;
      return names.get(i);
    });
    return gen;
  }

  private SchemaBuilder.OrganizationBuilder newOrgBuilderWithMetric(SchemaBuilder builder)
    throws IOException {
    return addMetric(builder.newOrg(ORG_ID));
  }

  private SchemaBuilder.OrganizationBuilder addMetric(SchemaBuilder.OrganizationBuilder builder)
    throws IOException {
    return builder.newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)
                  .withBoolean("bField").withAlias("aliasname").asField()
                  .withString("sField").asField()
                  .build();
  }
}
