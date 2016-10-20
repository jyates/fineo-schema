package io.fineo.lambda.handle.schema.metric.create;

import io.fineo.client.model.schema.metric.CreateMetricRequest;
import io.fineo.client.model.schema.metric.ReadMetricResponse;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.delete.TestDeleteMetric;
import io.fineo.lambda.handle.schema.metric.read.TestReadMetric;
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
    } catch (RuntimeException e) {
      HandlerTestUtils.expectError(e, 400, "Bad Request");
    }
  }

  @Test
  public void testFailIfNoOrgName() throws Exception {
    CreateMetricRequestInternal create = new CreateMetricRequestInternal();
    HandlerTestUtils
      .failNoValue(manager -> new CreateMetricHandler(manager, new UpdateRetryer(), 1), create);
  }

  @Test
  public void testFailIfNoMetricName() throws Exception {
    CreateMetricRequestInternal create = new CreateMetricRequestInternal();
    create.setOrgId("org");
    HandlerTestUtils
      .failNoValue(manager -> new CreateMetricHandler(manager, new UpdateRetryer(), 1), create);
  }

  @Test
  public void testFailWithDuplicateMetricTypes() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    TestCreateOrg.createOrg(store, org);
    String metric = "metricname";
    createMetric(store, org, metric);
    CreateMetricRequestInternal create = new CreateMetricRequestInternal();
    create.setOrgId("org");
    create.setBody(new CreateMetricRequest().setMetricName(metric));
    HandlerTestUtils.failWithProvider(() -> new StoreManager(store),
      manager -> new CreateMetricHandler(manager, new UpdateRetryer(), 1), create, 400,
      "Bad Request");
  }

  @Test
  public void testCreateDeleteCreate() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    TestCreateOrg.createOrg(store, org);
    String metric = "metricname";
    createMetric(store, org, metric);
    TestDeleteMetric.deleteMetric(store, org, metric);
    createMetric(store, org, metric);
    io.fineo.lambda.handle.schema.metric.read.ReadMetricResponse
      response = TestReadMetric.read(store, org, metric);
  }

  public static void createMetric(SchemaStore store, String org, String metric) throws Exception {
    // create a new metric
    CreateMetricRequestInternal create = new CreateMetricRequestInternal();
    create.setOrgId(org);
    CreateMetricRequest body = new CreateMetricRequest();
    body.setMetricName(metric);
    create.setBody(body);
    CreateMetricHandler handler =
      new CreateMetricHandler(() -> new StoreManager(store), new UpdateRetryer(), 1);
    handler.handle(create, null);
  }
}
