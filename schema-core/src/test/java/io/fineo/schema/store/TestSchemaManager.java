package io.fineo.schema.store;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.MetricMetadata;
import io.fineo.schema.FineoStopWords;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.exception.SchemaTypeNotFoundException;
import org.apache.avro.Schema;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.store.SchemaTestUtils.generateStringNames;
import static io.fineo.schema.store.timestamp.MultiPatternTimestampParser.TimeFormats.ISO_INSTANT;
import static io.fineo.schema.store.timestamp.MultiPatternTimestampParser.TimeFormats
  .RFC_1123_DATE_TIME;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSchemaManager {

  private static final String orgId = "org1", metricName = "metricname";

  private static final String STRING = "STRING";
  public static final String[] BAD_FIELD_NAMES =
    new String[]{"_f", "_f1", "_fd",
      "T0" + FineoStopWords.PREFIX_DELIMITER,
      "T0" + FineoStopWords.PREFIX_DELIMITER + "field"};

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreateOrg() throws Exception {
    SchemaStore store = getStore();
    List<String> names = newArrayList("n1", "n2", "n3", "n4");
    StoreManager manager = new StoreManager(generateStringNames(names), store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    StoreClerk.Metric metric = getOnlyOneMetric(store, orgId);
    // verify metric info
    assertEquals(names.get(0), metric.getMetricId());
    assertEquals(metricName, metric.getUserName());
    // verify field info
    assertEquals(newArrayList(names.get(1)), metric.getCanonicalFieldNames());
    assertEquals(newArrayList(f(fieldName, Schema.Type.STRING, of(), names.get(1))), metric
      .getUserVisibleFields
        ());
    assertEquals(fieldName, metric.getUserFieldNameFromCanonicalName(names.get(1)));
  }

  private StoreClerk.Metric getOnlyOneMetric(SchemaStore store, String orgId) {
    StoreClerk clerk = new StoreClerk(store, orgId);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals(1, metrics.size());
    return metrics.get(0);
  }

  @Test
  public void testUpdateMetricNameInOrg() throws Exception {
    SchemaStore store = getStore();
    List<String> names = newArrayList("n1", "n2", "n3", "n4");
    StoreManager manager = new StoreManager(generateStringNames(names), store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    String metricName2 = "new metric name";
    manager.updateOrg(orgId).updateMetric(metricName).setDisplayName(metricName2).build().commit();

    StoreClerk.Metric metric = getOnlyOneMetric(store, orgId);
    assertEquals(metricName2, metric.getUserName());
  }

  @Test
  public void testUpdateMetricAliasInOrg() throws Exception {
    SchemaStore store = getStore();
    List<String> names = newArrayList("n1", "n2", "n3", "n4");
    StoreManager manager = new StoreManager(generateStringNames(names), store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    String metricName2 = "alias metric name";
    manager.updateOrg(orgId).updateMetric(metricName).addAliases(metricName2).build().commit();

    StoreClerk.Metric metric = getOnlyOneMetric(store, orgId);
    assertEquals(metricName, metric.getUserName());
    StoreClerk clerk = new StoreClerk(store, orgId);
    assertEquals(metric, clerk.getMetricForUserNameOrAlias(metricName2));
  }


  @Test
  public void testUpdateFieldAliases() throws Exception {
    SchemaStore store = getStore();
    List<String> names = newArrayList("n1", "n2", "n3", "n4");
    StoreManager manager = new StoreManager(generateStringNames(names), store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    String fieldAlias = "alias_field";
    manager.updateOrg(orgId).updateMetric(metricName).addFieldAlias(fieldName, fieldAlias)
           .build().commit();

    StoreClerk.Metric metric = getOnlyOneMetric(store, orgId);
    String canonicalFieldName = names.get(1);
    assertEquals(newArrayList(canonicalFieldName), metric.getCanonicalFieldNames());
    assertEquals(newArrayList(f(fieldName, Schema.Type.STRING, newArrayList(fieldAlias), names
        .get(1))),
      metric.getUserVisibleFields());
    assertEquals(canonicalFieldName, metric.getCanonicalNameFromUserFieldName(fieldAlias));
  }

  @Test
  public void testAddMetricWithNoFieldsToOrg() throws Exception {
    SchemaStore store = getStore();
    List<String> names = newArrayList("n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = generateStringNames(names);
    StoreManager manager = new StoreManager(gen, store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    String metric2name = "metric2name", metric2Alias = "alias - metric2";
    manager = new StoreManager(gen, store);
    manager.updateOrg(orgId).newMetric()
           .setDisplayName(metric2name)
           .addAliases(metric2Alias).build().commit();

    StoreClerk clerk = new StoreClerk(store, orgId);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals(2, metrics.size());
    // first metric
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
    assertEquals(newArrayList(f(fieldName, Schema.Type.STRING, of(), names.get(1))), metric
      .getUserVisibleFields());
    // second, added metric
    StoreClerk.Metric metric2 = clerk.getMetricForUserNameOrAlias(metric2name);
    assertNotEquals("Didn't find added metric: " + metric2name, metric2);
    assertNotEquals(metric, metric2);
    assertEquals(metric2, clerk.getMetricForUserNameOrAlias(metric2Alias));
    assertEquals(0, metric2.getCanonicalFieldNames().size());
  }

  @Test
  public void testAddMetricWithFieldToOrg() throws Exception {
    SchemaStore store = getStore();
    List<String> names = newArrayList("n1", "n2", "n3", "n4");
    SchemaNameGenerator gen = generateStringNames(names);
    StoreManager manager = new StoreManager(gen, store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    String metric2name = "metric2name", metric2Alias = "alias - metric2";
    manager = new StoreManager(gen, store);
    manager.updateOrg(orgId).newMetric()
           .setDisplayName(metric2name)
           .addAliases(metric2Alias)
           // add a single field
           .newField().withName(fieldName).withType(StoreManager.Type.STRING).build()
           .build().commit();

    StoreClerk clerk = new StoreClerk(store, orgId);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals(2, metrics.size());
    // first metric
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
    StoreClerk.Field typedField = f(fieldName, Schema.Type.STRING, names.get(1));
    assertEquals(newArrayList(typedField), metric.getUserVisibleFields());
    // second, added metric
    StoreClerk.Metric metric2 = clerk.getMetricForUserNameOrAlias(metric2name);
    assertNotEquals("Didn't find added metric: " + metric2name, metric2);
    assertNotEquals(metric, metric2);
    assertEquals(metric2, clerk.getMetricForUserNameOrAlias(metric2Alias));
    StoreClerk.Field typedField2 = f(fieldName, Schema.Type.STRING, names.get(3));
    assertEquals(newArrayList(typedField2), metric2.getUserVisibleFields());
  }

  @Test(expected = NullPointerException.class)
  public void testFieldMustHaveNameOrExceptionOnBuild() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    String org = "org1", metricName = "metricname";
    manager.newOrg(org).newMetric().setDisplayName(metricName).newField().withType(STRING).build();
  }


  @Test(expected = NullPointerException.class)
  public void testFieldMustHaveTypeOrExceptionOnBuild() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    String org = "org1", metricName = "metricname", name = "f1";
    manager.newOrg(org).newMetric().setDisplayName(metricName).newField().withName(name).build();
  }

  @Test(expected = SchemaTypeNotFoundException.class)
  public void testFieldMustHaveValidTypeOrExceptionOnBuild() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    String org = "org1", metricName = "metricname", name = "f1";
    manager.newOrg(org).newMetric().setDisplayName(metricName)
           .newField().withName(name).withType("NOT_A_TYPE").build();
  }

  @Test
  public void testBadFieldNames() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(SchemaNameGenerator.DEFAULT_INSTANCE, store);
    String orgId = "org1", metricName = "metricname";
    expectAllBadFields();
    List<Pair<String, String>> fields =
      newArrayList(BAD_FIELD_NAMES).stream().map(f -> p(f, STRING)).collect(Collectors.toList());
    commitSimpleType(manager, orgId, metricName, of(), fields.toArray(new Pair[0]));
  }

  @Test
  public void testFailBadFieldAlias() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(SchemaNameGenerator.DEFAULT_INSTANCE, store);
    String orgId = "org1", metricName = "metricname";
    StoreManager.OrganizationBuilder builder = manager.newOrg(orgId);
    StoreManager.MetricBuilder mb = builder.newMetric().setDisplayName(metricName);
    mb.newField()
      .withName("field")
      .withType(STRING)
      .withAliases(asList(BAD_FIELD_NAMES)).build();
    expectAllBadFields();
    mb.build().commit();
  }

  @Test
  public void testBadMetricNames() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(SchemaNameGenerator.DEFAULT_INSTANCE, store);
    String orgId = "org1";
    StoreManager.OrganizationBuilder builder = manager.newOrg(orgId);
    for (String name : BAD_FIELD_NAMES) {
      builder.newMetric().setDisplayName(name).build();
    }
    expectAllBadFields();
    builder.commit();
  }

  @Test
  public void testBadMetricAliases() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(SchemaNameGenerator.DEFAULT_INSTANCE, store);
    String orgId = "org1", metricName = "metricname";
    StoreManager.OrganizationBuilder builder = manager.newOrg(orgId);
    StoreManager.MetricBuilder mb = builder.newMetric().setDisplayName(metricName);
    expectAllBadFields();
    mb.addAliases(BAD_FIELD_NAMES).build().commit();
  }

  @Test
  public void testMetricAliases() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(SchemaNameGenerator.DEFAULT_INSTANCE, store);
    String orgId = "org1", metricName = "metricname";
    StoreManager.OrganizationBuilder builder = manager.newOrg(orgId);
    StoreManager.MetricBuilder mb = builder.newMetric().setDisplayName(metricName);
    String a1 = "alias1", a2 = "alias2";
    mb.addAliases(a1, a2);
    mb.build().commit();

    StoreClerk clerk = new StoreClerk(store, orgId);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
    assertEquals(newArrayList(a1, a2), metric.getAliases());
  }

  @Test
  public void testDeleteField() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(SchemaNameGenerator.DEFAULT_INSTANCE, store);
    String orgId = "org1", metricName = "metricname", field = "f1";
    manager.newOrg(orgId).newMetric().setDisplayName(metricName).newField().withName(field)
           .withType(StoreManager.Type.BOOLEAN).build().build().commit();

    StoreClerk clerk = new StoreClerk(store, orgId);
    assertEquals(1, clerk.getMetricForUserNameOrAlias(metricName).getUserVisibleFields().size());

    // delete the field
    manager.updateOrg(orgId).updateMetric(metricName).deleteField(field).build().commit();
    assertEquals(0, clerk.getMetricForUserNameOrAlias(metricName).getUserVisibleFields().size());
    List<FieldMetadata> fields = clerk.getMetricForUserNameOrAlias(metricName).getHiddenFields();
    assertEquals(1, fields.size());
    FieldMetadata meta = fields.get(0);
    assertEquals(field, meta.getDisplayName());
    assertTrue(meta.getHiddenTime() > 0);
  }

  @Test
  public void testCreationHasTimestampFieldMetadata() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(orgId).newMetric().setDisplayName(metricName).build().commit();

    StoreClerk clerk = new StoreClerk(store, orgId);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
    MetricMetadata metricMetadata = metric.getUnderlyingMetric().getMetadata();
    FieldMetadata timestamp = metricMetadata.getFields().get(AvroSchemaProperties.TIMESTAMP_KEY);
    assertNotNull("No timestamp field found in metric metadata", timestamp);
    assertTrue("Timestamp field is not an internal field!", timestamp.getInternalField());
  }

  @Test
  public void testUpdateTimestampField() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(orgId).newMetric().setDisplayName(metricName).build().commit();
    String alias = "ts-alias";
    manager.updateOrg(orgId).updateMetric(metricName).addFieldAlias(AvroSchemaProperties
      .TIMESTAMP_KEY, alias).build().commit();

    StoreClerk clerk = new StoreClerk(store, orgId);
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
    MetricMetadata metricMetadata = metric.getUnderlyingMetric().getMetadata();
    FieldMetadata timestamp = metricMetadata.getFields().get(AvroSchemaProperties.TIMESTAMP_KEY);
    assertEquals(newArrayList(AvroSchemaProperties.TIMESTAMP_KEY, alias), timestamp
      .getFieldAliases());
  }

  @Test
  public void testValidTimestampParsingSpecification() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    thrown.expect(IllegalArgumentException.class);
    manager.newOrg(orgId)
           .withTimestampFormat("a/c/v")
           .newMetric().setDisplayName(metricName)
           .build().commit();
  }

  /**
   * Able to just specify the names of the specs, rather than a pattern
   */
  @Test
  public void testSpecifyTimestampParsingSpecificationNames() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(orgId)
           .withTimestampFormat(RFC_1123_DATE_TIME.name())
           .newMetric().setDisplayName(metricName)
           .withTimestampFormat(ISO_INSTANT.name())
           .build().commit();

    StoreClerk clerk = new StoreClerk(store, orgId);
    assertEquals(newArrayList(ISO_INSTANT.name()),
      clerk.getMetricForUserNameOrAlias(metricName).getTimestampPatterns());

    // setting a new format should just have that format
    manager.updateOrg(orgId)
           .updateMetric(metricName)
           .withTimestampFormat(RFC_1123_DATE_TIME.name())
           .build().commit();
    assertEquals(newArrayList(RFC_1123_DATE_TIME.name()),
      clerk.getMetricForUserNameOrAlias(metricName).getTimestampPatterns());
  }

  private void expectAllBadFields() {
    thrown.expect(RuntimeException.class);
    thrown.expect(expectFailedFields(BAD_FIELD_NAMES));
  }

  public static Matcher<String> expectFailedFields(String... fields) {
    return new BaseMatcher<String>() {
      private String msg;
      private List<String> error = newArrayList(fields);

      @Override
      public boolean matches(Object item) {
        msg = ((RuntimeException) item).getMessage();
        Preconditions.checkNotNull(msg, "No message attached to error! \nCause: %s, \nStack: %s",
          item, Arrays.toString(((RuntimeException) item).getStackTrace()));
        for (String field : fields) {
          if (msg.contains(field)) {
            this.error.remove(field);
          }
        }
        return this.error.isEmpty();
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("failures for fields").appendValue(asList(fields));
      }
    };
  }

  private static <K, V> Pair<K, V> p(K k, V v) {
    return new Pair<>(k, v);
  }

  public static void commitSimpleType(StoreManager manager, String orgId, String metricName,
    List<String> metricAliases, Pair<String, String>... fieldNameAndType)
    throws IOException, OldSchemaException {
    StoreManager.OrganizationBuilder builder = manager.newOrg(orgId);
    StoreManager.MetricBuilder mb = builder.newMetric().setDisplayName(metricName);
    for (String alias : metricAliases) {
      mb.addAliases(alias);
    }
    for (Pair<String, String> nameAndType : fieldNameAndType) {
      mb.newField()
        .withName(nameAndType.getKey())
        .withType(nameAndType.getValue())
        .build();
    }
    mb.build().commit();
  }

  public StoreClerk.Field f(String username, Schema.Type type, String cname) {
    return f(username, type, of(), cname);
  }

  public StoreClerk.Field f(String username, Schema.Type type, List<String> aliases, String cname) {
    return new StoreClerk.Field(username, type, aliases, cname);
  }

  private SchemaStore getStore() {
    return new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  }
}
