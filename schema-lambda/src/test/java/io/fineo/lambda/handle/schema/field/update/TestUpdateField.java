package io.fineo.lambda.handle.schema.field.update;

import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.lambda.handle.schema.metric.field.TestAddField;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestUpdateField {

  @Test
  public void testUpdateField() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    TestCreateOrg.createOrg(store, org);
    String metric = "metricname";
    TestCreateMetric.createMetric(store, org, metric);
    String field = "field", type = "STRING";
    TestAddField.createField(store, org, metric, field, type);

    String alias = "fieldalias";
    updateField(store, org, metric, field, alias);
    StoreClerk.Field f = HandlerTestUtils.getOnlyFirstField(org, metric, store);
    assertEquals(newArrayList(alias), f.getAliases());
  }

  @Test
  public void testNoMissingFields() throws Exception {
    UpdateFieldRequest request = new UpdateFieldRequest();
    HandlerTestUtils.failNoValue(TestUpdateField::handler, request);
    request.setOrgId("o");
    HandlerTestUtils.failNoValue(TestUpdateField::handler, request);
    request.setMetricName("m");
    HandlerTestUtils.failNoValue(TestUpdateField::handler, request);
    request.setAliases(new String[]{"al"});
    HandlerTestUtils.failNoValue(TestUpdateField::handler, request);
  }

  public static void updateField(SchemaStore store, String org, String metric, String field,
    String... aliases) throws Exception {
    UpdateFieldRequest request = new UpdateFieldRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    request.setFieldName(field);
    request.setAliases(aliases);
    UpdateFieldHandler handler = handler(() -> new StoreManager(store));
    handler.handle(request, null);
  }

  private static UpdateFieldHandler handler(Provider<StoreManager> provider) {
    return new UpdateFieldHandler(provider, new UpdateRetryer(), 1);
  }
}
