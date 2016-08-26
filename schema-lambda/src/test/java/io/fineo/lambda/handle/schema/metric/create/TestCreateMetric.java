package io.fineo.lambda.handle.schema.metric.create;

import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestCreateMetric {

  @Test
  public void testCreateMetric() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    TestCreateOrg.createOrg(store, org);

    // create a new metric
    String metric = "metricname";
    createMetric(store, org, metric);

    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals("Wrong number of metrics. Got list: " + metrics, 1, metrics.size());
    StoreClerk.Metric m = metrics.get(0);
    assertEquals(org, m.getOrgId());
    assertEquals(metric, m.getUserName());
    assertEquals(of(), m.getUserVisibleFields());
  }

  @Test
  public void testDoubleCreateMetricFails() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    TestCreateOrg.createOrg(store, org);

    // create a new metric
    String metric = "metricname";
    createMetric(store, org, metric);
    try {
      createMetric(store, org, metric);
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testFailIfNoOrgName() throws Exception {
    CreateMetricRequest create = new CreateMetricRequest();
    HandlerTestUtils
      .failNoValue(manager -> new CreateMetricHandler(manager, new UpdateRetryer(), 1), create);
  }

  @Test
  public void testFailIfNoMetricName() throws Exception {
    CreateMetricRequest create = new CreateMetricRequest();
    create.setOrgId("org");
    HandlerTestUtils
      .failNoValue(manager -> new CreateMetricHandler(manager, new UpdateRetryer(), 1), create);
  }

  public static void createMetric(SchemaStore store, String org, String metric) throws Exception {
    // create a new metric
    CreateMetricRequest create = new CreateMetricRequest();
    create.setOrgId(org);
    create.setMetricName(metric);
    CreateMetricHandler handler =
      new CreateMetricHandler(() -> new StoreManager(store), new UpdateRetryer(), 1);
    handler.handle(create, null);
  }
}
