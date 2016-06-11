package io.fineo.schema.store;

import io.fineo.schema.OldSchemaException;
import io.fineo.schema.Pair;
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

/**
 *
 */
public class TestSchemaManager {

  private static final String STRING = "STRING";

  @Test
  public void testCreateOrg() throws Exception {
    SchemaStore store = getStore();
    List<String> names = newArrayList("n1", "n2", "n3", "n4");
    SchemaManager manager = new SchemaManager(generateStringNames(names), store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    StoreClerk.Metric metric = getOnlyOneMetric(store, orgId);
    // verify metric info
    assertEquals(names.get(0), metric.getMetricId());
    assertEquals(metricName, metric.getUserName());
    // verify field info
    assertEquals(newArrayList(names.get(1)), metric.getCanonicalFieldNames());
    assertEquals(newArrayList(p(fieldName, Schema.Type.STRING)), metric.getUserVisibleFields());
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
    SchemaManager manager = new SchemaManager(generateStringNames(names), store);
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
    SchemaManager manager = new SchemaManager(generateStringNames(names), store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    String metricName2 = "alias metric name";
    manager.updateOrg(orgId).updateMetric(metricName).addAliases(metricName2).build().commit();

    StoreClerk.Metric metric = getOnlyOneMetric(store, orgId);
    assertEquals(metricName2, metric.getUserName());
  }


  @Test
  public void testUpdateFieldAliases() throws Exception {
    SchemaStore store = getStore();
    List<String> names = newArrayList("n1", "n2", "n3", "n4");
    SchemaManager manager = new SchemaManager(generateStringNames(names), store);
    String orgId = "org1", metricName = "metricname", fieldName = "f1";
    commitSimpleType(manager, orgId, metricName, of(), p(fieldName, STRING));

    String fieldAlias = "alias_field";
    manager.updateOrg(orgId).updateMetric(metricName).addFieldAlias(fieldName, fieldAlias)
           .build().commit();

    StoreClerk.Metric metric = getOnlyOneMetric(store, orgId);
    assertEquals(newArrayList(names.get(1)), metric.getCanonicalFieldNames());
    assertEquals(newArrayList(p(fieldName, Schema.Type.STRING)), metric.getUserVisibleFields());
  }

  private static <K, V> Pair<K, V> p(K k, V v) {
    return new Pair<>(k, v);
  }


  private void commitSimpleType(SchemaManager manager, String orgId, String metricName,
    List<String> metricAliases, Pair<String, String>... fieldNameAndType)
    throws IOException, OldSchemaException {
    SchemaManager.OrganizationBuilder builder = manager.newOrg(orgId);
    SchemaManager.MetricBuilder mb = builder.newMetric().setDisplayName(metricName);
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

  private SchemaStore getStore() {
    return new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  }
}
