package io.fineo.lambda.handle.schema.metric.delete;

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
    request.setMetricUserName(metric);
    DeleteMetricHandler handler =
      new DeleteMetricHandler(() -> new StoreManager(store), new UpdateRetryer(), 1);
    handler.handle(request, null);

    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertTrue(metrics.isEmpty());
  }
}
