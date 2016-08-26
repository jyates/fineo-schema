package io.fineo.lambda.handle.schema.metric.update;

import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestUpdateMetric {

  @Test
  public void testUpdateMetricDisplayName() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metric = "metricname", metric2 = "newmetric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
    UpdateMetricRequest request = new UpdateMetricRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    request.setNewDisplayName(metric2);

    UpdateMetricHandler handler = createHandler(() -> new StoreManager(store));
    handler.handle(request, null);

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
    UpdateMetricRequest request = new UpdateMetricRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    request.setAliases(new String[]{metric2});

    UpdateMetricHandler handler = createHandler(() -> new StoreManager(store));
    handler.handle(request, null);

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
    HandlerTestUtils.failNoValue(this::createHandler, request);
    request.setOrgId("org");
    HandlerTestUtils.failNoValue(this::createHandler, request);
  }

  private UpdateMetricHandler createHandler(Provider<StoreManager> provider) {
    return new UpdateMetricHandler(provider, new UpdateRetryer(), 1);
  }
}
