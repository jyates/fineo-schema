package io.fineo.lambda.handle.schema.metric.update;

import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.schema.MapRecord;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.AvroSchemaEncoderFactory;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import io.fineo.schema.store.timestamp.MultiPatternTimestampParser;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.store.timestamp.MultiPatternTimestampParser.TimeFormats
  .RFC_1123_DATE_TIME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestUpdateMetric {

  @Test
  public void testUpdateMetricDisplayName() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metric = "metricname", metric2 = "newmetric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    updateMetricDisplayName(store, org, metric, metric2);

    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals("Wrong number of metrics! Got: " + metrics, 1, metrics.size());
    StoreClerk.Metric m = metrics.get(0);
    assertEquals(org, m.getOrgId());
    assertEquals(metric2, m.getUserName());
    assertEquals(m, clerk.getMetricForUserNameOrAlias(metric));
  }

  @Test
  public void testUpdateMetricAliases() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metric = "metricname", metric2 = "newmetric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    updateMetricAliases(store, org, metric, metric2);

    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals("Wrong number of metrics! Got: " + metrics, 1, metrics.size());
    StoreClerk.Metric m = metrics.get(0);
    assertEquals(org, m.getOrgId());
    assertEquals(metric, m.getUserName());
    assertEquals(m, clerk.getMetricForUserNameOrAlias(metric2));
  }

  @Test
  public void testMissingRequestFields() throws Exception {
    UpdateMetricRequest request = new UpdateMetricRequest();
    HandlerTestUtils.failNoValue(TestUpdateMetric::createHandler, request);
    request.setOrgId("org");
    HandlerTestUtils.failNoValue(TestUpdateMetric::createHandler, request);
  }

  @Test
  public void testUpdateKeyAliases() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metric = "metricname", metricKey = "newkey";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
    addMetricKeyAliases(store, org, metric, metricKey);
    StoreClerk clerk = new StoreClerk(store, org);
    assertReadMetricForKey(store, org, metric, metricKey);

    removeMetricKeyAliases(store, org, metric, metricKey);
    tryReadMetricNotFound(clerk, metric, metricKey);

    // add and remove at the same time
    handleRequest(store, org, metric, request -> {
      request.setNewKeys(new String[]{metricKey, "anotherkey"});
      request.setRemoveKeys(new String[]{"anotherkey"});
    });
    assertReadMetricForKey(store, org, metric, metricKey);
    tryReadMetricNotFound(clerk, metric, "anotherkey");
  }

  @Test
  public void testRemoveKeyForOtherMetricFails() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metric = "metricname", metric2 = "metricname2", metricKey = "newkey";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
    TestCreateMetric.createMetric(store, org, metric2);
    addMetricKeyAliases(store, org, metric, metricKey);
    try {
      removeMetricKeyAliases(store, org, metric2, metricKey);
      fail("Shouldn't have been able to remove alias for metric that doesn't have that alias");
    } catch (IllegalArgumentException e) {
      // expected
    }
    assertReadMetricForKey(store, org, metric, metricKey);
  }

  @Test
  public void testUpdateMetricTimestampPatterns() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metric = "metricname";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
    String name = MultiPatternTimestampParser.TimeFormats.RFC_1123_DATE_TIME.name();
    setTimestampPatterns(store, org, metric, name);

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric m = clerk.getMetricForUserNameOrAlias(metric);
    assertEquals(newArrayList(name), m.getTimestampPatterns());
  }

  private void assertReadMetricForKey(SchemaStore store, String org, String metric, String
    metricKey) throws SchemaNotFoundException {
    StoreClerk clerk = new StoreClerk(store, org);
    Map<String, Object> map = new HashMap<>();
    map.put(metricKey, metric);
    MapRecord mapRecord = new MapRecord(map);
    AvroSchemaEncoderFactory.RecordMetric rm = clerk.getEncoderFactory().getMetricForRecord
      (mapRecord);
    assertEquals(metric, rm.metricAlias);
  }

  private void tryReadMetricNotFound(StoreClerk clerk, String metric, String metricKey)
    throws SchemaNotFoundException {
    try {
      Map<String, Object> map = new HashMap<>();
      map.put(metricKey, metric);
      clerk.getEncoderFactory().getMetricForRecord(new MapRecord(map));
      fail("Should not have been able to read metric without key!");
    } catch (NullPointerException e) {
      //expected
    }
  }

  private static UpdateMetricHandler createHandler(Provider<StoreManager> provider) {
    return new UpdateMetricHandler(provider, new UpdateRetryer(), 1);
  }

  public static void updateMetricAliases(SchemaStore store, String org, String metric,
    String... aliases) throws Exception {
    handleRequest(store, org, metric, request -> request.setAliases(aliases));
  }

  public static void addMetricKeyAliases(SchemaStore store, String org, String metric,
    String... keys) throws Exception {
    handleRequest(store, org, metric, request -> request.setNewKeys(keys));
  }

  public static void removeMetricKeyAliases(SchemaStore store, String org, String metric,
    String... keys) throws Exception {
    handleRequest(store, org, metric, request -> request.setRemoveKeys(keys));
  }

  public static void updateMetricDisplayName(SchemaStore store, String org, String metric,
    String name) throws Exception {
    handleRequest(store, org, metric, request -> request.setNewDisplayName(name));
  }

  public static void setTimestampPatterns(SchemaStore store, String org, String metric, String
    ... patterns) throws Exception {
    handleRequest(store, org, metric, request -> {
      request.setTimestampPatterns(patterns);
    });
  }

  private static void handleRequest(SchemaStore store, String org, String metric,
    Consumer<UpdateMetricRequest> update) throws Exception {
    UpdateMetricRequest request = new UpdateMetricRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    update.accept(request);

    UpdateMetricHandler handler = createHandler(() -> new StoreManager(store));
    handler.handle(request, null);
  }
}
