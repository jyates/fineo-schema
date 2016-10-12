package io.fineo.lambda.handle.schema.field.update;

import com.google.inject.Provider;
import io.fineo.client.model.schema.field.UpdateFieldRequest;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.field.read.ReadFieldResponse;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.lambda.handle.schema.metric.field.TestAddField;
import io.fineo.lambda.handle.schema.metric.read.ReadMetricResponse;
import io.fineo.lambda.handle.schema.metric.read.TestReadMetric;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestUpdateField {

  public static final String TIMESTAMP = "timestamp";

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
    UpdateFieldRequestInternal request = new UpdateFieldRequestInternal();
    HandlerTestUtils.failNoValue(TestUpdateField::handler, request);
    request.setOrgId("o");
    HandlerTestUtils.failNoValue(TestUpdateField::handler, request);
    UpdateFieldRequest body = new UpdateFieldRequest();
    body.setMetricName("m");
    request.setBody(body);
    HandlerTestUtils.failNoValue(TestUpdateField::handler, request);
    body.setAliases(new String[]{"al"});
    request.setBody(body);
    HandlerTestUtils.failNoValue(TestUpdateField::handler, request);
  }

  @Test
  public void testUpdateTimestampFieldAlias() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    TestCreateOrg.createOrg(store, org);
    String metric = "metricname";
    TestCreateMetric.createMetric(store, org, metric);

    String alias = "fieldalias";
    updateField(store, org, metric, TIMESTAMP, alias);
    ReadMetricResponse read = TestReadMetric.read(store, org, metric);
    Optional<ReadFieldResponse> ts =
      Arrays.asList(read.getFields()).stream().filter(f -> f.getName().equals(TIMESTAMP))
            .findAny();
    assertTrue("Missing timestamp field in response!", ts.isPresent());
    ReadFieldResponse timestamp = ts.get();
    assertArrayEquals(new String[]{alias}, timestamp.getAliases());
  }

  public static void updateField(SchemaStore store, String org, String metric, String field,
    String... aliases) throws Exception {
    UpdateFieldRequestInternal request = new UpdateFieldRequestInternal();
    request.setOrgId(org);
    UpdateFieldRequest body = new UpdateFieldRequest();
    body.setMetricName(metric);
    body.setFieldName(field);
    body.setAliases(aliases);
    request.setBody(body);
    UpdateFieldHandler handler = handler(() -> new StoreManager(store));
    handler.handle(request, null);
  }

  private static UpdateFieldHandler handler(Provider<StoreManager> provider) {
    return new UpdateFieldHandler(provider, new UpdateRetryer(), 1);
  }
}
