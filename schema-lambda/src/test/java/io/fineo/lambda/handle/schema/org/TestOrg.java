package io.fineo.lambda.handle.schema.org;

import com.google.inject.Provider;
import io.fineo.client.model.schema.ReadSchemaManagement;
import io.fineo.client.model.schema.SchemaManagementRequest;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.lambda.handle.schema.org.read.ReadOrgHandler;
import io.fineo.lambda.handle.schema.org.read.ReadOrgResponse;
import io.fineo.lambda.handle.schema.org.update.UpdateOrgHandler;
import io.fineo.lambda.handle.schema.org.update.UpdateOrgResponse;
import io.fineo.lambda.handle.schema.response.OrgResponse;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import io.fineo.schema.timestamp.MultiPatternTimestampParser;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.function.BiConsumer;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestOrg {

  @Test
  public void testFailIfOrgDoesNotExist() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    ExternalOrgRequest request = new ExternalOrgRequest();
    request.setOrgId("not an org");
    request.setGet(new ReadSchemaManagement());
    HandlerTestUtils.failWithProvider(() -> store, TestOrg::createHandler, request,
      500, "Internal Server Error");
  }

  @Test
  public void testUpdateKeyAliases() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid", metricKey = "newkey";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, "metricname");
    setMetricKeys(store, org, metricKey);

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Tenant tenant = clerk.getTenat();
    assertEquals(newArrayList(metricKey), tenant.getMetricKeyAliases());
    assertMetricKeysEquals(store, org, metricKey);

    // change the set of metric keys and ensure it completely changes
    String metricKey2 = "anotherkey";
    setMetricKeys(store, org, metricKey2);
    assertMetricKeysEquals(store, org, metricKey2);

    // and that we preserve order
    setMetricKeys(store, org, metricKey2, metricKey);
    assertMetricKeysEquals(store, org, metricKey2, metricKey);
    setMetricKeys(store, org, metricKey, metricKey2);
    assertMetricKeysEquals(store, org, metricKey, metricKey2);

    // and its kind of on you if you have duplicates
    setMetricKeys(store, org, metricKey2, metricKey2);
    assertMetricKeysEquals(store, org, metricKey2, metricKey2);
  }

  private static void assertMetricKeysEquals(SchemaStore store, String org, String... keys)
    throws Exception {
    ReadOrgResponse read = read(store, org);
    assertArrayEquals(keys, read.getMetricKeys());
  }

  @Test
  public void testUpdateTimestampPatterns() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "orgid";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, "metricname");
    String pattern = MultiPatternTimestampParser.TimeFormats.ISO_DATE_TIME.name();
    setTimestampPatterns(store, org, pattern);

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Tenant tenant = clerk.getTenat();
    assertEquals(newArrayList(pattern), tenant.getTimestampPatterns());
    assertTimestampPatternEquals(store, org, pattern);

    // changing the pattern is fine
    String pattern2 = MultiPatternTimestampParser.TimeFormats.ISO_INSTANT.name();
    setTimestampPatterns(store, org, pattern2);
    assertTimestampPatternEquals(store, org, pattern2);

    // and order is preserved
    setTimestampPatterns(store, org, pattern, pattern2);
    assertTimestampPatternEquals(store, org, pattern, pattern2);
    setTimestampPatterns(store, org, pattern2, pattern);
    assertTimestampPatternEquals(store, org, pattern2, pattern);

    // and you are on your own for duplicates
    setTimestampPatterns(store, org, pattern, pattern);
    assertTimestampPatternEquals(store, org, pattern, pattern);
  }

  @Test
  public void testFailInternalErrors() throws Exception {
    ExternalOrgRequest external = new ExternalOrgRequest();
    HandlerTestUtils.fail500(TestOrg::createHandler, external);
    external.setType("NOT_A_TYPE");
    HandlerTestUtils.failNoValueWithProvider(TestOrg::createHandler, external);
    external.setOrgId("org");
    HandlerTestUtils.fail500(TestOrg::createHandler, external);
    external.setType("patch");
    HandlerTestUtils.fail500(TestOrg::createHandler, external);
  }

  @Test
  public void testFailMalformedRead() throws Exception {
    ExternalOrgRequest external = new ExternalOrgRequest();
    external.setType("get");
    external.setGet(new ReadSchemaManagement());
    HandlerTestUtils.failNoValueWithProvider(TestOrg::createHandler, external);
  }

  @Test
  public void testFailMalformedUpdate() throws Exception {
    ExternalOrgRequest external = new ExternalOrgRequest();
    external.setType("patch");
    external.setPatch(new SchemaManagementRequest());
    HandlerTestUtils.failNoValueWithProvider(TestOrg::createHandler, external);
  }

  private static void assertTimestampPatternEquals(SchemaStore store, String org, String...
    patterns) throws Exception {
    ReadOrgResponse read = read(store, org);
    assertArrayEquals(patterns, read.getTimestampPatterns());
  }

  public static ReadOrgResponse read(SchemaStore store, String org) throws Exception {
    return handleRequest(store, org, new ReadSchemaManagement(), (ex, get) -> ex.setGet(get));
  }

  public static UpdateOrgResponse setMetricKeys(SchemaStore store, String org, String... keys)
    throws Exception {
    return handleRequest(store, org, new SchemaManagementRequest(), (ex, patch) -> {
      ex.setPatch(patch);
      patch.setMetricTypeKeys(keys);
    });
  }

  public static UpdateOrgResponse setTimestampPatterns(SchemaStore store, String org, String...
    patterns)
    throws Exception {
    return handleRequest(store, org, new SchemaManagementRequest(), (ex, patch) -> {
      ex.setPatch(patch);
      patch.setTimestampPatterns(patterns);
    });
  }

  private static <REQUEST, RESPONSE extends OrgResponse> RESPONSE handleRequest(
    SchemaStore store, String org, REQUEST request,
    BiConsumer<ExternalOrgRequest, REQUEST> update) throws Exception {
    ExternalOrgRequest external = new ExternalOrgRequest();
    external.setOrgId(org);
    update.accept(external, request);
    String type = null;
    if (external.getGet() != null) {
      type = "GET";
    } else if (external.getPatch() != null) {
      type = "PATCH";
    }
    external.setType(type);

    OrgHandler handler = createHandler(() -> store);
    return (RESPONSE) handler.handle(external, null);
  }

  public static OrgHandler createHandler(Provider<SchemaStore> store) {
    Provider<StoreManager> manager = () -> new StoreManager(store.get());
    return new OrgHandler(new RequestRunner(manager, new UpdateRetryer(), 1),
      new UpdateOrgHandler(manager),
      new ReadOrgHandler(store));
  }
}
