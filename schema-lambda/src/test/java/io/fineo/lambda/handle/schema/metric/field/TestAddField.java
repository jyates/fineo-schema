package io.fineo.lambda.handle.schema.metric.field;

import com.google.inject.Provider;
import io.fineo.client.model.schema.field.CreateFieldRequest;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.apache.avro.Schema;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class TestAddField {

  @Test
  public void testAddField() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    String metric = "metricname";
    createOrgAndMetric(org, metric, store);

    String field = "field1", type = "STRING";
    createField(store, org, metric, field, type);

    StoreClerk.Field f = HandlerTestUtils.getOnlyFirstField(org, metric, store);
    assertEquals(field, f.getName());
    assertEquals(Schema.Type.STRING, f.getType());
    assertEquals(newArrayList(), f.getAliases());

    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals(1, metrics.size());
    StoreClerk.Metric storedMetric = metrics.get(0);
    List<StoreClerk.Field> fields = storedMetric.getUserVisibleFields();
    assertEquals(1, fields.size());
    StoreClerk.Field storedField = fields.get(0);
    assertEquals(field, storedField.getName());
    assertEquals(Schema.Type.STRING, storedField.getType());
  }

  public static void createField(SchemaStore store, String org, String metric, String field,
    String type, String... aliases) throws Exception {
    AddFieldToMetricRequestInternal request = new AddFieldToMetricRequestInternal();
    request.setOrgId(org);
    CreateFieldRequest body = new CreateFieldRequest();
    body.setMetricName(metric);
    body.setFieldName(field);
    body.setFieldType(type);
    if (aliases != null && aliases.length > 0) {
      body.setAliases(aliases);
    }
    request.setBody(body);

    AddFieldToMetricHandler handler = handler(store);
    handler.handle(request, null);
  }

  @Test
  public void testAddFieldWithAliases() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    String metric = "metricname";
    createOrgAndMetric(org, metric, store);

    String field = "field1", type = "STRING";
    String alias = "fieldalias";
    createField(store, org, metric, field, type, alias);

    StoreClerk.Field f = HandlerTestUtils.getOnlyFirstField(org, metric, store);
    assertEquals(field, f.getName());
    assertEquals(Schema.Type.STRING, f.getType());
    assertEquals(newArrayList(alias), f.getAliases());
  }

  @Test
  public void testFailOnMissingFields() throws Exception {
    AddFieldToMetricRequestInternal request = new AddFieldToMetricRequestInternal();
    HandlerTestUtils.failNoValue(TestAddField::handler, request);
    request.setOrgId("org");
    HandlerTestUtils.failNoValue(TestAddField::handler, request);
    CreateFieldRequest body = new CreateFieldRequest();
    body.setMetricName("metric");
    request.setBody(body);
    HandlerTestUtils.failNoValue(TestAddField::handler, request);
    body.setFieldName("field");
    HandlerTestUtils.failNoValue(TestAddField::handler, request);
  }

  @Test
  public void testFailOnDuplicate() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    String metric = "metricname";
    createOrgAndMetric(org, metric, store);

    String field = "field1", type = "STRING";
    createField(store, org, metric, field, type);
    try {
      createField(store, org, metric, field, type);
      fail();
    } catch (RuntimeException e) {
      HandlerTestUtils.expectError(e, 400, "Bad Request");
    }
  }

  private static void createOrgAndMetric(String org, String metric, SchemaStore store)
    throws Exception {
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
  }

  private static AddFieldToMetricHandler handler(SchemaStore store) {
    return handler(() -> new StoreManager(store));
  }

  private static AddFieldToMetricHandler handler(Provider<StoreManager> provider) {
    return new AddFieldToMetricHandler(provider, new UpdateRetryer(), 1);
  }
}
