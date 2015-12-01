package io.fineo.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.avro.SchemaUtils;
import io.fineo.schema.store.SchemaBuilder;
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
import static org.junit.Assert.fail;

/**
 * Test that we build logical organization, metric type and field schemas as expected.
 */
public class TestSchemaBuilder {

  private static final String ORG_ID = "orgId";
  public static final String NEW_SCHEMA_DISPLAY_NAME = "newschema";

  @Test
  public void testNewOrg() throws Exception {
    List<String> names = Lists.newArrayList("n0", "n1", "n2");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    Metadata orgMetadata = verifyGeneratedMetadata(organization, names);

    // read the schema out and ensure we get the fields we added
    Metric metricSchema = verifyGeneratedMetricMetadata(organization, names);
    // verify each field name + alias in the actual schema
    Map<String, List<String>> fields =
      metricSchema.getMetadata().getMetricTypes().getCanonicalNamesToAliases();
    assertEquals("Expected 1 base fields and 2 added", 3, fields.size());
    // verify the fields we added
    assertEquals(Lists.newArrayList("aliasname", "bField"), fields.get(names.get(1)));
    assertEquals(Lists.newArrayList("sField"), fields.get(names.get(2)));
    assertEquals(new ArrayList<>(), fields.get("unknown_fields"));

    // verify the stored fields
    Map<String, String> fieldNameTypes = new HashMap<>();
    // check the fields that we added. Don't verify the 'hidden' fields b/c that is covered in
    // other tests and then we get dependent test failures
    fieldNameTypes.put(names.get(1), "boolean");
    fieldNameTypes.put(names.get(2), "string");
    verifySchemaHasFields(orgMetadata, metricSchema, fieldNameTypes, 3);
  }

  @Test
  public void testAddMetricTypeToOrg() throws Exception {
    List<String> names = Lists.newArrayList("n0", "n1", "n2");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);

    names = Lists.newArrayList("r0", "r1", "r2");
    gen = setupMockNameGenerator(names);
    builder = new SchemaBuilder(gen);
    // should be able to add a new schema with the same 'name' (but different canonical name)
    organization = addMetricType(builder.updateOrg(organization)).build();
    Metadata metadata = organization.getMetadata();
    assertEquals(ORG_ID, metadata.getCanonicalName());
    Map<String, List<String>> types =
      metadata.getMetricTypes().getCanonicalNamesToAliases();
    assertEquals("Didn't get expected number of fields!", 2, types.size());
    assertEquals("Didn't get expected canonical names", Sets.newHashSet("n0", "r0"),
      types.keySet());
  }

  @Test
  public void testUpdateMetric() throws Exception {
    // build our standard org
    List<String> names = Lists.newArrayList("n0", "n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);

    // then update a metric field in the org
    String metadataName = names.get(0);
    String fieldName = names.get(1);
    SchemaBuilder.OrganizationBuilder orgBuilder = builder.updateOrg(organization)
                                                          .updateSchema(metadataName)
                                                          .updateField(fieldName)
                                                          .withName("other alias").asField()
                                                          .withInt("iField").asField()
                                                          .build();
    organization = orgBuilder.build();

    Metadata orgMetadata = organization.getMetadata();
    // org metadata is still the same
    Map<String, List<String>> metricAliasMap =
      verifyGeneratedOrgMetadata(organization, metadataName);
    // check that we correctly changed the fields
    assertEquals(1, organization.getSchemas().size());
    Metric metric = organization.getSchemas().get(metricAliasMap.keySet().iterator().next());
    Map<String, List<String>> fieldMap =
      metric.getMetadata().getMetricTypes().getCanonicalNamesToAliases();
    Map<String, List<String>> expectedFields = getBaseExpectedAliases();
    expectedFields.put("n1", Lists.newArrayList("aliasname", "bField", "other alias"));
    expectedFields.put("n2", Lists.newArrayList("sField"));
    expectedFields.put("n3", Lists.newArrayList("iField"));
    assertEquals(expectedFields, fieldMap);

    // added field aliases are correct
    List<String> aliases = fieldMap.get(names.get(3));
    assertEquals(Lists.newArrayList("iField"), aliases);

    // verify the record schema has the correct types - the two original and the one added
    Map<String, String> fields = new HashMap<>();
    fields.put(names.get(1), "boolean");
    fields.put(names.get(2), "string");
    fields.put(names.get(3), "int");
    verifySchemaHasFields(orgMetadata, metric, fields, 4);
  }

  @Test
  public void testHideField() throws Exception {
    List<String> names = Lists.newArrayList("n0", "n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = new SchemaBuilder(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);
    Metric metricSchema = verifyGeneratedMetricMetadata(organization, names);
    String schemaName = metricSchema.getMetadata().getCanonicalName();

    organization = builder.updateOrg(organization)
                          .updateSchema(schemaName)
                            .updateField("n1").softDelete().build()
                          .build();
    // metadata doesn't change
    verifyGeneratedMetadata(organization, names);
    metricSchema = organization.getSchemas().get(schemaName);
    assertEquals(1, metricSchema.getHiddenTime().size());
    assertEquals(Sets.newHashSet("n1"), metricSchema.getHiddenTime().keySet());
  }

  private void verifySchemaHasFields(Metadata metadata, Metric metric,
    Map<String, String> fieldCnameToTypes, int totalFields) {
    String metricSchemaFullName = SchemaUtils.getCustomerSchemaFullName(metadata.getCanonicalName(),
      metric.getMetadata().getCanonicalName());
    Schema schema =
      SchemaUtils.parseSchema(metric.getMetricSchema(), metricSchemaFullName);
    assertNotNull("Didn't get a schema for name: " + metricSchemaFullName, schema);
    assertEquals(totalFields, schema.getFields().size());
    int[] contains = new int[1];
    schema.getFields().stream().forEach(schemaField -> {
      String type = fieldCnameToTypes.get(schemaField.name());
      if (type != null) {
        assertEquals(type, schemaField.schema().getType().getName());
        contains[0]++;
      }
    });
    assertEquals(fieldCnameToTypes.size(), contains[0]);
  }

  private Metadata verifyGeneratedMetadata(SchemaBuilder.Organization organization,
    List<String> names) {
    Metadata orgMetadata = organization.getMetadata();
    String name = names.get(0);
    verifyGeneratedOrgMetadata(organization, name);
    assertEquals("Wrong number of metric schemas created!", 1, organization.getSchemas().size());
    Metric schema = organization.getSchemas().values().iterator().next();
    assertEquals("Org metadata field canonical name does not match name in schema", name,
      schema.getMetadata().getCanonicalName());
    return orgMetadata;
  }

  private Map<String, List<String>> verifyGeneratedOrgMetadata(SchemaBuilder.Organization org,
    String metricName) {
    Metadata orgMetadata = org.getMetadata();
    assertEquals("Wrong org id stored!", ORG_ID, orgMetadata.getCanonicalName());
    Map<String, List<String>> fields =
      orgMetadata.getMetricTypes().getCanonicalNamesToAliases();
    assertEquals("Wrong number of metric type fields", 1, fields.size());
    CharSequence field = fields.keySet().iterator().next();
    assertEquals("Canonical metric name is not the expected generated name!", metricName, field);
    assertEquals("Expected an alias for metric cname: " + metricName, 1,
      fields.get(metricName).size());
    return fields;
  }

  private Metric verifyGeneratedMetricMetadata(SchemaBuilder.Organization org,
    List<String> names) {
    String schemaCName = names.get(0);
    Metric schema = org.getSchemas().get(schemaCName);
    assertEquals("Canonical name was not the expected mock-generated name", schemaCName,
      schema.getMetadata().getCanonicalName());
    Map<String, List<String>> expectedAliases = getBaseExpectedAliases();
    expectedAliases.put(names.get(1), Lists.newArrayList("aliasname", "bField"));
    expectedAliases.put(names.get(2), Lists.newArrayList("sField"));
    assertEquals("Field -> Alias mapping didn't match", expectedAliases,
      schema.getMetadata().getMetricTypes().getCanonicalNamesToAliases());
    return schema;
  }

  private Map<String, List<String>> getBaseExpectedAliases() {
    Map<String, List<String>> expectedAliases = new HashMap<>();
    expectedAliases.put(SchemaBuilder.UNKNOWN_KEYS_FIELD, new ArrayList<>());
    return expectedAliases;
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