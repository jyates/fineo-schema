package io.fineo.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.avro.SchemaUtils;
import org.apache.avro.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test that we build logical organization, metric type and field schemas as expected.
 */
public class TestSchemaBuilder {

  private static final String ORG_ID = "orgId";
  public static final String NEW_SCHEMA_DISPLAY_NAME = "newschema";

  @Test
  public void testNewOrg() throws Exception {
    List<String> names = Lists.newArrayList("n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    Metadata orgMetadata = verifyGeneratedMetadata(organization, names);

    // read the schema out and ensure we get the fields we added
    Metric metricSchema = verifyGeneratedMetricMetadata(organization, names);
    // verify each field name + alias
    Map<CharSequence, List<CharSequence>> fields =
      metricSchema.getFields().getCanonicalNamesToAliases();
    assertEquals("Expected 3 base fields and 2 added", 5, fields.size());
    // verify the fields we added
    assertEquals(Lists.newArrayList("sField"), fields.get(names.get(1)));
    assertEquals(Lists.newArrayList("aliasname", "bField"), fields.get(names.get(0)));
    assertEquals(new ArrayList<>(), fields.get("hidden"));
    assertEquals(new ArrayList<>(), fields.get("hidden_time"));
    assertEquals(new ArrayList<>(), fields.get("unknown_fields"));

    // verify the stored fields
    String metricSchemaFullName = SchemaUtils
      .getCustomerSchemaFullName(orgMetadata.getOrgId(), names.get(2));
    Schema schema =
      SchemaUtils.parseSchema(metricSchema.getMetricSchema(), metricSchemaFullName);
    assertNotNull("Didn't get a schema for name: " + metricSchemaFullName, schema);
    assertEquals(5, schema.getFields().size());
    // check the fields that we added. Don't verify the 'hidden' fields b/c that is covered in
    // other tests and then we get dependent test failures
    List<Schema.Field> schemaFields = schema.getFields();
    int[] contains = new int[1];
    Map<String, String> fieldNameTypes = new HashMap<>();
    fieldNameTypes.put("bField", "boolean");
    fieldNameTypes.put("sField", "string");
    schemaFields.stream().forEach(schemaField -> {
      String type = fieldNameTypes.get(schemaField.name());
      if (type != null) {
        assertEquals(type, schemaField.getProp("type"));
      }
      contains[0]++;
    });
  }

  private Metadata verifyGeneratedMetadata(SchemaBuilder.Organization organization,
    List<String> names) {
    Metadata orgMetadata = organization.getMetadata();
    assertEquals("Wrong org id stored!", ORG_ID, orgMetadata.getOrgId());
    Map<CharSequence, List<CharSequence>> fields =
      orgMetadata.getMetricTypes().getCanonicalNamesToAliases();
    assertEquals("Wrong number of metric type fields", 1, fields.size());
    CharSequence field = fields.keySet().iterator().next();
    assertEquals("Canonical metric name is not the last generated name!",
      names.get(names.size() - 1), field);
    assertEquals(0, fields.get(field).size());
    assertEquals("Wrong number of metric schemas created!", 1, organization.getSchemas().size());
    Metric schema = organization.getSchemas().get(0);
    assertEquals("Org metadata field canonical name does not match name in schema", field,
      schema.getCanonicalName());
    return orgMetadata;
  }

  private Metric verifyGeneratedMetricMetadata(SchemaBuilder.Organization org,
    List<String> names) {
    Metric schema = org.getSchemas().get(0);
    assertEquals(names.get(names.size() - 1), schema.getCanonicalName());
    assertEquals(1, schema.getFields().getCanonicalNamesToAliases().size());
    assertEquals(NEW_SCHEMA_DISPLAY_NAME,
      schema.getFields().getCanonicalNamesToAliases().entrySet().iterator().next());
    return schema;
  }

  @Test
  public void testUpdateOrgFieldName() throws Exception {
    List<String> names = Lists.newArrayList("n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);

    names = Lists.newArrayList("r1", "r2", "r3", "r4");
    gen = setupMockNameGenerator(names);
    builder = new SchemaBuilder(gen);
    // should be able to add a new schema with the same 'name' (but different canonical name)
    organization = addMetricType(builder.updateOrg(organization)).build();
    Metadata metadata = organization.getMetadata();
    assertEquals(ORG_ID, metadata.getOrgId());
    Map<CharSequence, List<CharSequence>> types =
      metadata.getMetricTypes().getCanonicalNamesToAliases();
    assertEquals("Didn't get expected number of fields!", 2, types.size());
    assertEquals("Didn't get expected canonical names", Sets.newHashSet("n4", "r4"),
      types.keySet());
  }

  @Test
  public void testUpdateMetric() throws Exception {
    // build our standard org
    List<String> names = Lists.newArrayList("n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);

    // then update a metric field in the org
    SchemaBuilder.OrganizationBuilder orgBuilder = builder.updateOrg(organization);
    SchemaBuilder.MetadataBuilder metadataBuilder = orgBuilder.updateSchema(names.get(3));

    organization = orgBuilder.build();

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

  private SchemaBuilder.OrganizationBuilder newOrgBuilderWithAMetricType(SchemaBuilder builder)
    throws IOException {
    return addMetricType(builder.newOrg(ORG_ID));
  }

  private SchemaBuilder.OrganizationBuilder addMetricType(SchemaBuilder.OrganizationBuilder builder)
    throws IOException {
    return builder.newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)
                  .withBoolean("bField").withAlias("aliasname").asField()
                  .withString("sField").asField()
                  .build();
  }
}
