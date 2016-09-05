package io.fineo.schema.store;

import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.avro.SchemaNameUtils;
import org.apache.avro.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test that we build logical organization, metric type and field schemas as expected.
 */
public class TestSchemaBuilder {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

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
    OrgMetadata orgMetadata = verifyGeneratedMetadata(organization, names);

    // read the schema out and ensure we get the fields we added
    Metric metricSchema = verifyGeneratedMetricMetadata(organization, names);
    // verify each field name + alias in the actual schema
    Map<String, FieldMetadata> fields = metricSchema.getMetadata().getFields();
    assertEquals("Expected 1 base fields and 2 added", 3, fields.size());
    // verify the fields we added
    assertEquals(newArrayList("bField", "aliasname"), fields.get(names.get(1)).getFieldAliases());
    assertEquals(newArrayList(STRING_FIELD_NAME), fields.get(names.get(2)).getFieldAliases());
    assertEquals(new ArrayList<>(),
      fields.get(AvroSchemaProperties.BASE_FIELDS_KEY).getFieldAliases());

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
             .updateField(fieldName).withName("other alias").asField()
             .withInt("iField").asField()
             .build();
    organization = orgBuilder.build();

    OrgMetadata orgMetadata = organization.getMetadata();
    // org metadata is still the same
    Map<String, OrgMetricMetadata> metricAliasMap =
      verifyGeneratedOrgMetadata(organization, metadataName);
    // check that we correctly changed the fields
    assertEquals(1, organization.getSchemas().size());
    Metric metric = organization.getSchemas().get(metricAliasMap.keySet().iterator().next());
    Map<String, FieldMetadata> fieldMap = metric.getMetadata().getFields();
    Map<String, List<String>> expectedFields = getBaseExpectedAliases();
    expectedFields.put("n1", newArrayList("bField", "aliasname", "other alias"));
    expectedFields.put("n2", newArrayList(STRING_FIELD_NAME));
    expectedFields.put("n3", newArrayList("iField"));
    assertEquals(expectedFields, SchemaTestUtils.mapFieldNames(metric.getMetadata()));

    // added field aliases are correct
    FieldMetadata aliases = fieldMap.get(names.get(3));
    assertEquals(newArrayList("iField"), aliases.getFieldAliases());

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
    String schemaName = metricSchema.getMetadata().getMeta().getCanonicalName();

    organization = builder.updateOrg(organization.getMetadata())
                          .updateSchema(metricSchema)
                          .updateField("n1").softDelete().build()
                          .build();
    // metadata doesn't change
    verifyGeneratedMetadata(organization, names);
    metricSchema = organization.getSchemas().get(schemaName);
    Map<String, FieldMetadata> fields = metricSchema.getMetadata().getFields();
    assertEquals("Got fields: " + fields, 3, fields.size());
    FieldMetadata field = fields.entrySet().stream()
                                .filter(e -> e.getKey().equals("n1"))
                                .map(e -> e.getValue())
                                .findFirst()
                                .get();
    assertTrue(field.getHiddenTime() > 0);
  }

  @Test
  public void testCreateMetadataWithNoFields() throws Exception {
    SchemaBuilder builder = SchemaBuilder.createForTesting(SchemaNameGenerator.DEFAULT_INSTANCE);
    String id = "123d43";
    String metricName = "newschema";
    SchemaBuilder.OrganizationBuilder metadata = builder.newOrg(id)
                                                        .newMetric().withName(metricName)
                                                        .build();
    SchemaBuilder.Organization organization = metadata.build();
    // verify that we have the org and the correct schema name mapping
    OrgMetadata orgMetadata = organization.getMetadata();
    assertEquals(id, orgMetadata.getMetadata().getCanonicalName());
    Map<String, OrgMetricMetadata> schemaNameMap = orgMetadata.getMetrics();
    assertEquals(1, schemaNameMap.size());
    assertEquals(newArrayList(metricName),
      schemaNameMap.values().iterator().next().getAliasValues());

    // look at the actual schema we created
    Map<String, Metric> schemas = organization.getSchemas();
    assertEquals(1, schemas.size());
    Metric schema = schemas.values().iterator().next();
    assertEquals(schemaNameMap.keySet().iterator().next(),
      schema.getMetadata().getMeta().getCanonicalName());
    Map<String, List<String>> expectedFields = getBaseExpectedAliases();
    assertEquals(expectedFields, SchemaTestUtils.mapFieldNames(schema.getMetadata()));
  }

  @Test
  public void testNoDuplicateMetricTypeAliases() throws Exception {
    List<String> names = newArrayList("n0", "n1", "n2", "n4", "n5", "n6");
    SchemaNameGenerator gen = setupMockNameGenerator(names);
    SchemaBuilder builder = SchemaBuilder.createForTesting(gen);
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    verifyGeneratedMetadata(organization, names);
    thrown.expect(IllegalArgumentException.class);
    thrown.reportMissingExceptionWithMessage(
      "Should not be able to add a metric with the same alias value as another metric,\n e.g you "
      + "cannot have two metrics: 't' and 'v', whose value for 'metrictype' field is the same. "
      + "\nThere would be no way to disambiguate between the two.");
    addMetricType(builder.updateOrg(organization.getMetadata()));
  }

  @Test
  public void testNoDuplicateMetricTypeAliasesOnUpdate() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization organization = newOrgBuilderWithAMetricType(builder).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.reportMissingExceptionWithMessage(
      "Should not be able to add a metric with the same alias value as another metric,\n e.g you "
      + "cannot have two metrics: 't' and 'v', whose value for 'metrictype' field is the same. "
      + "\nThere would be no way to disambiguate between the two.");
    builder.updateOrg(organization.getMetadata())
           .newMetric().withName(NEW_SCHEMA_DISPLAY_NAME).build();
  }

  /**
   * Check that when we create a metric we don't attempt to create two fields with the same alias
   *
   * @throws Exception
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNoDuplicateFieldNameAliasesInSameMetric() throws Exception {
    SchemaBuilder.create().newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
                 .withBoolean(BOOLEAN_FIELD_NAME).asField()
                 .withFloat(BOOLEAN_FIELD_NAME).asField()
                 .build().build();
  }

  @Test
  public void testDuplicateMetricNamesGetIgnored() throws Exception {
    SchemaBuilder.Organization org = addBooleanField(
      SchemaBuilder.create().newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
                   .withName(NEW_SCHEMA_DISPLAY_NAME)).build().build();
    assertOneMetricName(NEW_SCHEMA_DISPLAY_NAME, org);
  }

  @Test
  public void testDuplicateMetricNamesInUpdateGetIgnored() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization org = addBooleanField(
      builder.newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
             .withName(NEW_SCHEMA_DISPLAY_NAME)).build().build();
    Metric metric = org.getSchemas().values().iterator().next();
    org =
      builder.updateOrg(org.getMetadata()).updateSchema(metric).withName(NEW_SCHEMA_DISPLAY_NAME)
             .build().build();
    assertOneMetricName(NEW_SCHEMA_DISPLAY_NAME, org);
  }

  @Test
  public void testDuplicateMetricNameAndDisplayNameHasOnlyOneName() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization org =
      builder.newOrg(ORG_ID).newMetric().withDisplayName(NEW_SCHEMA_DISPLAY_NAME)
             .withName(NEW_SCHEMA_DISPLAY_NAME).build().build();
    assertEquals(newArrayList(NEW_SCHEMA_DISPLAY_NAME),
      SchemaTestUtils.collectMapListValues(SchemaTestUtils.mapAliasValueNames(org.getMetadata())));
  }

  private void assertOneMetricName(String metricName, SchemaBuilder.Organization org) {
    assertEquals(newArrayList(metricName),
      SchemaTestUtils.collectMapListValues(SchemaTestUtils.mapAliasValueNames(org.getMetadata())));
  }

  @Test
  public void testDuplicateFieldNamesGetIgnored() throws Exception {
    SchemaBuilder.Organization org =
      SchemaBuilder.create().newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
                   .withLong(BOOLEAN_FIELD_NAME).withAlias(BOOLEAN_FIELD_NAME).asField().build()
                   .build();
    Metric metric = org.getSchemas().values().iterator().next();
    assertEquals(newArrayList(BOOLEAN_FIELD_NAME),
      SchemaTestUtils.collectMapListValues(SchemaTestUtils.mapFieldNames(metric.getMetadata())));
  }

  /**
   * Check that when we update a metric we don't attempt to create two fields with the same alias.
   *
   * @throws Exception
   */
  @Test
  public void testNoDuplicateFieldNameAliasesInUpdatedMetric() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization org = builder.newOrg(ORG_ID)
                                            .newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
                                            .withBoolean(BOOLEAN_FIELD_NAME).asField().build()
                                            .build();
    Metric metric = org.getSchemas().values().iterator().next();

    thrown.expect(IllegalArgumentException.class);
    thrown.reportMissingExceptionWithMessage("Should not be able to add a field with the same "
                                             + "alias as another field in the same metric");
    builder.updateOrg(org.getMetadata()).updateSchema(metric)
           .withFloat(BOOLEAN_FIELD_NAME).asField()
           .build().build();
  }

  @Test
  public void testUpdateSchemaDisplayName() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization org = addBooleanField(
      builder.newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
             .withName(NEW_SCHEMA_DISPLAY_NAME)).build().build();
    Metric metric = org.getSchemas().values().iterator().next();
    String newName = "new display name";
    org =
      builder.updateOrg(org.getMetadata()).updateSchema(metric).withDisplayName(newName)
             .build().build();
    List<String> names = SchemaTestUtils.collectMapListValues(SchemaTestUtils.mapAliasValueNames(org
      .getMetadata()));
    Collections.sort(names);
    List<String> expected = newArrayList(newName, NEW_SCHEMA_DISPLAY_NAME);
    Collections.sort(expected);
    assertEquals(expected, names);
    assertEquals(newName,
      org.getMetadata()
         .getMetrics().get(metric.getMetadata().getMeta().getCanonicalName())
         .getDisplayName());
  }

  @Test
  public void testUpdateFieldDisplayName() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.Organization org =
      builder.newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
             .withBoolean(BOOLEAN_FIELD_NAME).asField().build().build();
    Metric metric = getFirstMetric(org);
    List<String> fieldIds = getFieldIds(metric);
    assertEquals("Got fields: " + fieldIds, 1, fieldIds.size());
    String fieldId = fieldIds.get(0);
    String newName = "new field name";
    org = builder.updateOrg(org.getMetadata()).updateSchema(metric).updateField(fieldId)
                 .withName(newName).asField().build().build();
    assertOneMetricName(NEW_SCHEMA_DISPLAY_NAME, org);
    metric = getFirstMetric(org);
    fieldIds = getFieldIds(metric);
    assertEquals("Should not have added a new field", newArrayList(fieldId), fieldIds);
    List<String> aliases = newArrayList(newName, BOOLEAN_FIELD_NAME);
    Collections.sort(aliases);
    List<String> actualAliases = SchemaTestUtils.collectMapListValues(SchemaTestUtils
      .mapFieldNames(metric.getMetadata()));
    Collections.sort(actualAliases);
    assertEquals(aliases, actualAliases);
    assertEquals(NEW_SCHEMA_DISPLAY_NAME,
      org.getMetadata().getMetrics().get(metric.getMetadata().getMeta().getCanonicalName())
         .getDisplayName());
  }

  private Metric getFirstMetric(SchemaBuilder.Organization org) {
    return org.getSchemas().values().iterator().next();
  }

  private List<String> getFieldIds(Metric metric) {
    return metric.getMetadata().getFields().keySet().stream()
                 .filter(name -> !AvroSchemaProperties.BASE_FIELDS_KEY.equals(name))
                 .collect(Collectors.toList());
  }

  @Test
  public void testDuplicateFieldNameAliasesDifferentMetrics() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    String metric2 = NEW_SCHEMA_DISPLAY_NAME + " 2";
    SchemaBuilder.OrganizationBuilder orgBuilder =
      addBooleanField(builder.newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)).build();
    SchemaBuilder.Organization organization =
      addBooleanField(orgBuilder.newMetric().withName(metric2)).build().build();

    // check that we have two schemas with two aliases (one each)
    assertEquals(2, organization.getSchemas().keySet().size());
    List<String> aliases = SchemaTestUtils
      .collectMapListValues(SchemaTestUtils.mapAliasValueNames(organization.getMetadata()));
    Collections.sort(aliases);
    assertEquals(newArrayList(NEW_SCHEMA_DISPLAY_NAME, metric2), aliases);
    //check that we have fields with duplicate names in each schema
    organization.getSchemas().values().stream().forEach(
      metric -> assertEquals(newArrayList(BOOLEAN_FIELD_NAME),
        SchemaTestUtils.collectMapListValues(SchemaTestUtils.mapFieldNames(metric.getMetadata()))));
  }

  @Test
  public void testChangeMetricAliases() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.OrganizationBuilder orgBuilder =
      addBooleanField(builder.newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)).build();
    SchemaBuilder.Organization organization = orgBuilder.build();

    Metric metric = organization.getSchemas().values().iterator().next();
    orgBuilder = builder.updateOrg(organization.getMetadata());
    String metricAlias = "metricAlias";
    SchemaBuilder.Organization org =
      orgBuilder.updateSchema(metric).withName(metricAlias).build().build();
    OrgMetricMetadata metricMetadata =
      org.getMetadata().getMetrics().get(metric.getMetadata().getMeta().getCanonicalName());
    assertEquals(newArrayList(NEW_SCHEMA_DISPLAY_NAME, metricAlias),
      metricMetadata.getAliasValues());
  }

  private void verifySchemaHasFields(OrgMetadata metadata, Metric metric,
    Map<String, String> fieldCnameToTypes, int totalFields) {
    String metricSchemaFullName = SchemaNameUtils
      .getCustomerSchemaFullName(metadata.getMetadata().getCanonicalName(),
        metric.getMetadata().getMeta().getCanonicalName());
    Schema schema =
      SchemaNameUtils.parseSchema(metric.getMetricSchema(), metricSchemaFullName);
    assertNotNull("Didn't get a schema for name: " + metricSchemaFullName, schema);
    assertEquals(totalFields, schema.getFields().size());
    int[] contains = new int[1];
    schema.getFields().stream().forEach(schemaField -> {
      String type = fieldCnameToTypes.get(schemaField.name());
      if (type != null) {
        SchemaTestUtils.verifyFieldType(type, schemaField);
        contains[0]++;
      }
    });
    assertEquals(fieldCnameToTypes.size(), contains[0]);
  }

  static OrgMetadata verifyGeneratedMetadata(SchemaBuilder.Organization organization,
    List<String> names) {
    OrgMetadata orgMetadata = organization.getMetadata();
    String name = names.get(0);
    verifyGeneratedOrgMetadata(organization, name);
    assertEquals("Wrong number of metric schemas created!", 1, organization.getSchemas().size());
    Metric schema = organization.getSchemas().values().iterator().next();
    assertEquals("Org metadata field canonical name does not match name in schema", name,
      schema.getMetadata().getMeta().getCanonicalName());
    return orgMetadata;
  }

  private static Map<String, OrgMetricMetadata> verifyGeneratedOrgMetadata(
    SchemaBuilder.Organization org, String metricName) {
    OrgMetadata orgMetadata = org.getMetadata();
    assertEquals("Wrong org id stored!", ORG_ID, orgMetadata.getMetadata().getCanonicalName());
    Map<String, OrgMetricMetadata> metrics = orgMetadata.getMetrics();
    assertEquals("Wrong number of metric type fields", 1, metrics.size());
    CharSequence metricCanonicalName = metrics.keySet().iterator().next();
    assertEquals("Canonical metric name is not the expected generated name!", metricName,
      metricCanonicalName);
    assertEquals(
      "Expected an alias for metric cname: " + metricName + ", but got: " + metrics.get
        (metricName).getAliasValues(),
      1, metrics.get(metricName).getAliasValues().size());
    return metrics;
  }

  static Metric verifyGeneratedMetricMetadata(SchemaBuilder.Organization org,
    List<String> names) {
    String schemaCName = names.get(0);
    Metric schema = org.getSchemas().get(schemaCName);
    assertEquals("Canonical name was not the expected mock-generated name", schemaCName,
      schema.getMetadata().getMeta().getCanonicalName());
    Map<String, List<String>> expectedAliases = getBaseExpectedAliases();
    expectedAliases.put(names.get(1), newArrayList("bField", "aliasname"));
    expectedAliases.put(names.get(2), newArrayList(STRING_FIELD_NAME));
    assertEquals("Field -> Alias mapping didn't match", expectedAliases,
      SchemaTestUtils.mapFieldNames(schema.getMetadata()));

    return schema;
  }

  private static Map<String, List<String>> getBaseExpectedAliases() {
    Map<String, List<String>> expectedAliases = new HashMap<>();
    expectedAliases.put(AvroSchemaProperties.BASE_FIELDS_KEY, new ArrayList<>());
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
    return builder.newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
                  .withBoolean(BOOLEAN_FIELD_NAME).withAlias(BOOLEAN_FIELD_ALIAS).asField()
                  .withString(STRING_FIELD_NAME).asField()
                  .build();
  }

  private SchemaBuilder.MetricBuilder addBooleanField(SchemaBuilder.MetricBuilder builder) {
    return builder.withBoolean(BOOLEAN_FIELD_NAME).asField();
  }
}
