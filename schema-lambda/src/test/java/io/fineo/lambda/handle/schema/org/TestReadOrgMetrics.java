package io.fineo.lambda.handle.schema.org;

import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.lambda.handle.schema.metric.update.TestUpdateMetric;
import io.fineo.lambda.handle.schema.org.metrics.ReadOrgMetricsHandler;
import io.fineo.lambda.handle.schema.org.metrics.ReadOrgMetricsResponse;
import io.fineo.lambda.handle.schema.org.read.ReadOrgRequest;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestReadOrgMetrics {

  @Test
  public void testReadOrgMetrics() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metricname = "metricname";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metricname);

    ReadOrgMetricsResponse response = read(store, org);
    Map<String, String> map = response.getIdToMetricName();
    assertEquals(1, map.size());
    assertEquals(metricname, map.values().iterator().next());

    // validate that its the same agains
    response = read(store, org);
    map = response.getIdToMetricName();
    assertEquals(1, map.size());
    assertEquals(metricname, map.values().iterator().next());
  }

  @Test
  public void testReadOrgMetricWithAnAlias() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metricname = "metricname";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metricname);

    // add an alias for our metric
    TestUpdateMetric.updateMetricAliases(store, org, metricname, "metricalias");

    ReadOrgMetricsResponse response = read(store, org);
    Map<String, String> map = response.getIdToMetricName();
    assertEquals(1, map.size());
    assertEquals(metricname, map.values().iterator().next());
  }

  @Test
  public void testReadMultipleOrgs() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metricname = "metricname", metric2 = "anothermetric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metricname);
    TestCreateMetric.createMetric(store, org, metric2);

    ReadOrgMetricsResponse response = read(store, org);
    Map<String, String> map = response.getIdToMetricName();
    assertEquals(2, map.size());
    Iterator<String> keys = map.keySet().iterator();
    assertNotEquals("Metrics have same id!", keys.next(), keys.next());
    List<String> names = new ArrayList<>(map.values());
    Collections.sort(names);
    assertEquals(newArrayList(metric2, metricname), names);
  }

  @Test
  public void testMissingOrgFails() throws Exception {
    ReadOrgRequest request = new ReadOrgRequest();
    HandlerTestUtils.failNoValueWithProvider(TestReadOrgMetrics::createHandler, request);
  }

  @Test
  public void testReadNamesButNoMetrics() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org";
    TestCreateOrg.createOrg(store, org);
    ReadOrgMetricsResponse response = read(store, org);
    assertTrue(response.getIdToMetricName().isEmpty());
  }


  public static ReadOrgMetricsResponse read(SchemaStore store, String org) throws Exception {
    ReadOrgRequest request = new ReadOrgRequest();
    request.setOrgId(org);
    return handleRequest(store, request);
  }

  private static ReadOrgMetricsResponse handleRequest(
    SchemaStore store, ReadOrgRequest request) throws Exception {
    ReadOrgMetricsHandler handler = createHandler(() -> store);
    return handler.handleRequest(request, null);
  }

  public static ReadOrgMetricsHandler createHandler(Provider<SchemaStore> store) {
    Provider<StoreManager> manager = () -> new StoreManager(store.get());
    return new ReadOrgMetricsHandler(store, new RequestRunner(manager, new UpdateRetryer(), 1));
  }

}
