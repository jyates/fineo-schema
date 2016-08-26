package io.fineo.lambda.handle.schema.metric.delete;

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
import static org.junit.Assert.assertTrue;


public class TestDeleteMetric {

  @Test
  public void testDelete() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    DeleteMetricRequest request = new DeleteMetricRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    DeleteMetricHandler handler = handler(() -> new StoreManager(store));
    handler.handle(request, null);

    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertTrue(metrics.isEmpty());
  }

  @Test
  public void testDeleteAndCreateWithSameName() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    DeleteMetricRequest request = new DeleteMetricRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    DeleteMetricHandler handler = handler(() -> new StoreManager(store));
    handler.handle(request, null);

    TestCreateMetric.createMetric(store, org, metric);
    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals(1, metrics.size());
  }

  @Test
  public void testDeleteMissingParameters() throws Exception {
    DeleteMetricRequest request = new DeleteMetricRequest();
    HandlerTestUtils.failNoValue(TestDeleteMetric::handler, request);
  }

  private static DeleteMetricHandler handler(Provider<StoreManager> provider) {
    return new DeleteMetricHandler(provider, new UpdateRetryer(), 1);
  }
}
