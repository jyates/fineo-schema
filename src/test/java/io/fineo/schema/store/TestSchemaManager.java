package io.fineo.schema.store;

import io.fineo.schema.OldSchemaException;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.SchemaNameGenerator;
import org.apache.avro.Schema;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.avro.SchemaTestUtils.generateStringNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestSchemaManager {

  private static final String STRING = "STRING";

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
    assertEquals(newArrayList(f(fieldName, Schema.Type.STRING)), metric.getUserVisibleFields());
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
    assertEquals(newArrayList(f(fieldName, Schema.Type.STRING, newArrayList(fieldAlias))),
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
    assertEquals(newArrayList(f(fieldName, Schema.Type.STRING)), metric.getUserVisibleFields());
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
           .newField().withName(fieldName).withType(STRING).build()
           .build().commit();

    StoreClerk clerk = new StoreClerk(store, orgId);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals(2, metrics.size());
    // first metric
    StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
    StoreClerk.Field typedField = f(fieldName, Schema.Type.STRING);
    assertEquals(newArrayList(typedField), metric.getUserVisibleFields());
    // second, added metric
    StoreClerk.Metric metric2 = clerk.getMetricForUserNameOrAlias(metric2name);
    assertNotEquals("Didn't find added metric: " + metric2name, metric2);
    assertNotEquals(metric, metric2);
    assertEquals(metric2, clerk.getMetricForUserNameOrAlias(metric2Alias));
    assertEquals(newArrayList(typedField), metric2.getUserVisibleFields());
  }

  private static <K, V> Pair<K, V> p(K k, V v) {
    return new Pair<>(k, v);
  }

  private void commitSimpleType(StoreManager manager, String orgId, String metricName,
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

  public StoreClerk.Field f(String username, Schema.Type type) {
    return f(username, type, newArrayList());
  }

  public StoreClerk.Field f(String username, Schema.Type type, List<String> aliases) {
    return new StoreClerk.Field(username, type, aliases);
  }

  private SchemaStore getStore() {
    return new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  }
}
