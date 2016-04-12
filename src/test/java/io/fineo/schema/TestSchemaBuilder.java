package io.fineo.schema;

import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.avro.SchemaNameUtils;
import io.fineo.schema.store.SchemaBuilder;
import org.apache.avro.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test that we build logical organization, metric type and field schemas as expected.
 */
public class TestSchemaBuilder {

  private static final String ORG_ID = "orgId";
  public static final String NEW_SCHEMA_DISPLAY_NAME = "newschema";
  private static final String BOOLEAN_FIELD_NAME = "bField";
  private static final String STRING_FIELD_NAME = "sField";
  private static final String BOOLEAN_FIELD_ALIAS = "aliasname";

  @Test
  public void testNewOrg() throws Exception {
    List<String> names = newArrayList("n0", "n1", "n2");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = SchemaBuilder.createForTesting(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    Metadata orgMetadata = verifyGeneratedMetadata(organization, names);

    // read the schema out and ensure we get the fields we added
    Metric metricSchema = verifyGeneratedMetricMetadata(organization, names);
    // verify each field name + alias in the actual schema
    Map<String, List<String>> fields =
      metricSchema.getMetadata().getCanonicalNamesToAliases();
    assertEquals("Expected 1 base fields and 2 added", 3, fields.size());
    // verify the fields we added
    assertEquals(newArrayList("bField", "aliasname"), fields.get(names.get(1)));
    assertEquals(newArrayList(STRING_FIELD_NAME), fields.get(names.get(2)));
    assertEquals(new ArrayList<>(), fields.get(AvroSchemaEncoder.BASE_FIELDS_KEY));

    // verify the stored fields
    Map<String, String> fieldNameTypes = new HashMap<>();
    // check the fields that we added. Don't verify the 'hidden' fields b/c that is covered in
    // other tests and then we get dependent test failures
    fieldNameTypes.put(names.get(1), "boolean");
    fieldNameTypes.put(names.get(2), "string");
    verifySchemaHasFields(orgMetadata, metricSchema, fieldNameTypes, 3);
  }

  @Test
  public void testUpdateMetric() throws Exception {
    // build our standard org
    List<String> names = newArrayList("n0", "n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = SchemaBuilder.createForTesting(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);

    // then update a metric field in the org
    String metadataName = names.get(0);
    String fieldName = names.get(1);
    SchemaBuilder.OrganizationBuilder orgBuilder =
      builder.updateOrg(organization.getMetadata())
             .updateSchema(organization.getSchemas().get(metadataName))
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
      metric.getMetadata().getCanonicalNamesToAliases();
    Map<String, List<String>> expectedFields = getBaseExpectedAliases();
    expectedFields.put("n1", newArrayList("other alias", "bField", "aliasname"));
    expectedFields.put("n2", newArrayList(STRING_FIELD_NAME));
    expectedFields.put("n3", newArrayList("iField"));
    assertEquals(expectedFields, fieldMap);

    // added field aliases are correct
    List<String> aliases = fieldMap.get(names.get(3));
    assertEquals(newArrayList("iField"), aliases);

    // verify the record schema has the correct types - the two original and the one added
    Map<String, String> fields = new HashMap<>();
    fields.put(names.get(1), "boolean");
    fields.put(names.get(2), "string");
    fields.put(names.get(3), "int");
    verifySchemaHasFields(orgMetadata, metric, fields, 4);
  }

  @Test
  public void testHideField() throws Exception {
    List<String> names = newArrayList("n0", "n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = SchemaBuilder.createForTesting(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);
    Metric metricSchema = verifyGeneratedMetricMetadata(organization, names);
    String schemaName = metricSchema.getMetadata().getCanonicalName();

    organization = builder.updateOrg(organization.getMetadata())
                          .updateSchema(metricSchema)
                          .updateField("n1").softDelete().build()
                          .build();
    // metadata doesn't change
    verifyGeneratedMetadata(organization, names);
    metricSchema = organization.getSchemas().get(schemaName);
    assertEquals(1, metricSchema.getHiddenTime().size());
    assertEquals(newHashSet("n1"), metricSchema.getHiddenTime().keySet());
  }

  @Test
  public void testCreateMetadataWithNoFields() throws Exception {
    SchemaBuilder builder = SchemaBuilder.createForTesting(new SchemaNameGenerator());
    String id = "123d43";
    String metricName = "newschema";
    SchemaBuilder.OrganizationBuilder metadata = builder.newOrg(id)
                                                        .newSchema().withName(metricName)
                                                        .build();
    SchemaBuilder.Organization organization = metadata.build();
    // verify that we have the org and the correct schema name mapping
    assertEquals(id, organization.getMetadata().getCanonicalName());
    Map<String, List<String>> schemaNameMap =
      organization.getMetadata().getCanonicalNamesToAliases();
    assertEquals(1, schemaNameMap.size());
    assertEquals(newArrayList(metricName), schemaNameMap.values().iterator().next());

    // look at the actual schema we created
    Map<String, Metric> schemas = organization.getSchemas();
    assertEquals(1, schemas.size());
    Metric schema = schemas.values().iterator().next();
    assertEquals(schemaNameMap.keySet().iterator().next(), schema.getMetadata().getCanonicalName());
    Map<String, List<String>> schemaFieldMap = schema.getMetadata().getCanonicalNamesToAliases();
    Map<String, List<String>> expectedFields = getBaseExpectedAliases();
    assertEquals(expectedFields, schemaFieldMap);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoDuplicateMetricTypeAliases() throws Exception {
    List<String> names = newArrayList("n0", "n1", "n2", "n4", "n5", "n6");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = SchemaBuilder.createForTesting(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);
    addMetricType(builder.updateOrg(organization.getMetadata()));
  }

  /**
   * Check that when we create a metric we don't attempt to create two fields with the same alias
   *
   * @throws Exception
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNoDuplicateFieldNameAliasesInSameMetric() throws Exception {
    SchemaBuilder.create().newOrg(ORG_ID).newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)
                 .withBoolean(BOOLEAN_FIELD_NAME).asField()
                 .withFloat(BOOLEAN_FIELD_NAME).asField()
                 .build().build();
  }

  @Test
  public void testDuplicateMetricNamesGetIgnored() throws Exception {
    SchemaBuilder.Organization org = addBooleanField(
      SchemaBuilder.create().newOrg(ORG_ID).newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)
                   .withName(NEW_SCHEMA_DISPLAY_NAME)).build().build();
    assertEquals(newArrayList(NEW_SCHEMA_DISPLAY_NAME),
      collectMapListValues(org.getMetadata().getCanonicalNamesToAliases()));
  }

  @Test
  public void testDuplicateMetricNamesInUpdateGetIgnored() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization org = addBooleanField(
      builder.newOrg(ORG_ID).newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)
             .withName(NEW_SCHEMA_DISPLAY_NAME)).build().build();
    Metric metric = org.getSchemas().values().iterator().next();
    org =
      builder.updateOrg(org.getMetadata()).updateSchema(metric).withName(NEW_SCHEMA_DISPLAY_NAME)
             .build().build();
    assertEquals(newArrayList(NEW_SCHEMA_DISPLAY_NAME),
      collectMapListValues(org.getMetadata().getCanonicalNamesToAliases()));
  }

  @Test
  public void testDuplicateFieldNamesGetIgnored() throws Exception {
    SchemaBuilder.Organization org =
      SchemaBuilder.create().newOrg(ORG_ID).newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)
                   .withLong(BOOLEAN_FIELD_NAME).withAlias(BOOLEAN_FIELD_NAME).asField().build()
                   .build();
    Metric metric = org.getSchemas().values().iterator().next();
    assertEquals(newArrayList(BOOLEAN_FIELD_NAME),
      collectMapListValues(metric.getMetadata().getCanonicalNamesToAliases()));
  }

  /**
   * Check that when we update a metric we don't attempt to create two fields with the same alias.
   *
   * @throws Exception
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNoDuplicateFieldNameAliasesInUpdatedMetric() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization org = builder.newOrg(ORG_ID)
                                            .newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)
                                            .withBoolean(BOOLEAN_FIELD_NAME).asField().build()
                                            .build();
    Metric metric = org.getSchemas().values().iterator().next();
    builder.updateOrg(org.getMetadata()).updateSchema(metric)
           .withFloat(BOOLEAN_FIELD_NAME).asField()
           .build().build();
  }

  @Test
  public void testUpdateSchemaDisplayName() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization org = addBooleanField(
      builder.newOrg(ORG_ID).newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)
             .withName(NEW_SCHEMA_DISPLAY_NAME)).build().build();
    Metric metric = org.getSchemas().values().iterator().next();
    String newName = "new display name";
    org =
      builder.updateOrg(org.getMetadata()).updateSchema(metric).withDisplayName(newName)
             .build().build();
    assertEquals("Order matters here - display name is the first name",
      newArrayList(newName, NEW_SCHEMA_DISPLAY_NAME),
      collectMapListValues(org.getMetadata().getCanonicalNamesToAliases()));
  }

  @Test
  public void testUpdateFieldDisplayName() throws Exception {
    throw new RuntimeException("Not yet implemented");
  }

  @Test
  public void testDuplicateFieldNameAliasesDifferentMetrics() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    String metric2 = NEW_SCHEMA_DISPLAY_NAME + " 2";
    SchemaBuilder.OrganizationBuilder orgBuilder =
      addBooleanField(builder.newOrg(ORG_ID).newSchema().withName(NEW_SCHEMA_DISPLAY_NAME)).build();
    SchemaBuilder.Organization organization = addBooleanField(
      orgBuilder.newSchema().withName(metric2)).build().build();

    // check that we have two schemas with two aliases (one each)
    assertEquals(2, organization.getSchemas().keySet().size());
    Map<String, List<String>> schemas = organization.getMetadata().getCanonicalNamesToAliases();
    List<String> aliases = collectMapListValues(schemas);
    Collections.sort(aliases);
    assertEquals(newArrayList(NEW_SCHEMA_DISPLAY_NAME, metric2), aliases);
    //check that we have fields with duplicate names in each schema
    organization.getSchemas().values().stream().forEach(metric -> {
      Map<String, List<String>> fieldAliases = metric.getMetadata().getCanonicalNamesToAliases();
      assertEquals(newArrayList(BOOLEAN_FIELD_NAME), collectMapListValues(fieldAliases));
    });
  }

  private <T> List<T> collectMapListValues(Map<?, List<T>> map) {
    return map.values().stream()
              .sequential()
              .flatMap(list -> list.stream())
              .collect(Collectors.toList());
  }

  private void verifySchemaHasFields(Metadata metadata, Metric metric,
    Map<String, String> fieldCnameToTypes, int totalFields) {
    String metricSchemaFullName = SchemaNameUtils
      .getCustomerSchemaFullName(metadata.getCanonicalName(),
        metric.getMetadata().getCanonicalName());
    Schema schema =
      SchemaNameUtils.parseSchema(metric.getMetricSchema(), metricSchemaFullName);
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

  static Metadata verifyGeneratedMetadata(SchemaBuilder.Organization organization,
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

  private static Map<String, List<String>> verifyGeneratedOrgMetadata(
    SchemaBuilder.Organization org,
    String metricName) {
    Metadata orgMetadata = org.getMetadata();
    assertEquals("Wrong org id stored!", ORG_ID, orgMetadata.getCanonicalName());
    Map<String, List<String>> fields =
      orgMetadata.getCanonicalNamesToAliases();
    assertEquals("Wrong number of metric type fields", 1, fields.size());
    CharSequence field = fields.keySet().iterator().next();
    assertEquals("Canonical metric name is not the expected generated name!", metricName, field);
    assertEquals("Expected an alias for metric cname: " + metricName, 1,
      fields.get(metricName).size());
    return fields;
  }

  static Metric verifyGeneratedMetricMetadata(SchemaBuilder.Organization org,
    List<String> names) {
    String schemaCName = names.get(0);
    Metric schema = org.getSchemas().get(schemaCName);
    assertEquals("Canonical name was not the expected mock-generated name", schemaCName,
      schema.getMetadata().getCanonicalName());
    Map<String, List<String>> expectedAliases = getBaseExpectedAliases();
    expectedAliases.put(names.get(1), newArrayList("bField", "aliasname"));
    expectedAliases.put(names.get(2), newArrayList(STRING_FIELD_NAME));
    assertEquals("Field -> Alias mapping didn't match", expectedAliases,
      schema.getMetadata().getCanonicalNamesToAliases());
    return schema;
  }

  private static Map<String, List<String>> getBaseExpectedAliases() {
    Map<String, List<String>> expectedAliases = new HashMap<>();
    expectedAliases.put(AvroSchemaEncoder.BASE_FIELDS_KEY, new ArrayList<>());
    return expectedAliases;
  }

  static SchemaNameGenerator setupMockNameGenerator(List<String> names) {
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
                  .withBoolean(BOOLEAN_FIELD_NAME).withAlias(BOOLEAN_FIELD_ALIAS).asField()
                  .withString(STRING_FIELD_NAME).asField()
                  .build();
  }

  private SchemaBuilder.MetricBuilder addBooleanField(SchemaBuilder.MetricBuilder builder) {
    return builder.withBoolean(BOOLEAN_FIELD_NAME).asField();
  }
}
