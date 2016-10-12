package io.fineo.lambda.handle.schema.metric.update;

import com.google.inject.Provider;
import io.fineo.client.model.schema.metric.UpdateMetricRequest;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import io.fineo.schema.timestamp.MultiPatternTimestampParser;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.List;
import java.util.function.Consumer;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

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
    UpdateMetricRequestInternal request = new UpdateMetricRequestInternal();
    HandlerTestUtils.failNoValue(TestUpdateMetric::createHandler, request);
    request.setOrgId("org");
    HandlerTestUtils.failNoValue(TestUpdateMetric::createHandler, request);
    request.setBody(new UpdateMetricRequest());
    HandlerTestUtils.failNoValue(TestUpdateMetric::createHandler, request);
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

  /**
   * A metric that isn't found should be a 404 error
   */
  @Test
  public void testReadMetricNotFound() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metric = "metricname", metric2 = "newmetric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    try {
      handleRequest(store, org, metric2, request -> {request.getBody().setNewDisplayName("m2");});
    } catch (Exception e) {
      HandlerTestUtils.expectError(e, 404, "Not Found");
    }
  }

  private static UpdateMetricHandler createHandler(Provider<StoreManager> provider) {
    return new UpdateMetricHandler(provider, new UpdateRetryer(), 1);
  }

  public static void updateMetricAliases(SchemaStore store, String org, String metric,
    String... aliases) throws Exception {
    handleRequest(store, org, metric, request -> request.getBody().setAliases(aliases));
  }


  public static void updateMetricDisplayName(SchemaStore store, String org, String metric,
    String name) throws Exception {
    handleRequest(store, org, metric, request -> request.getBody().setNewDisplayName(name));
  }

  public static void setTimestampPatterns(SchemaStore store, String org, String metric, String
    ... patterns) throws Exception {
    handleRequest(store, org, metric, request -> {
      request.getBody().setTimestampPatterns(patterns);
    });
  }

  private static void handleRequest(SchemaStore store, String org, String metric,
    Consumer<UpdateMetricRequestInternal> update) throws Exception {
    UpdateMetricRequestInternal request = new UpdateMetricRequestInternal();
    request.setOrgId(org);
    request.setBody(new UpdateMetricRequest().setMetricName(metric));
    update.accept(request);

    UpdateMetricHandler handler = createHandler(() -> new StoreManager(store));
    handler.handleRequest(request, null);
  }
}
