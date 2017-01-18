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
  public void testUpdateFieldAliases() throws Exception {
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

  @Test
  public void testUpdateFieldDisplayName() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    TestCreateOrg.createOrg(store, org);
    String metric = "metricname";
    TestCreateMetric.createMetric(store, org, metric);
    String field = "field", type = "STRING";
    TestAddField.createField(store, org, metric, field, type);

    String newDisplayName = "newfield";
    UpdateFieldRequest request = new UpdateFieldRequest()
      .setNewDisplayName(newDisplayName)
      .setFieldName(field)
      .setMetricName(metric);
    ReadMetricResponse read = writeAndReadMetric(store, org, request);
    ReadFieldResponse fieldResponse = getFieldWithName(read, newDisplayName);
    assertEquals(newDisplayName, fieldResponse.getName());
    assertArrayEquals(new String[]{field}, fieldResponse.getAliases());
  }

  /**
   * This is a little bit of a strange edge case. The aliases include the display name, but here
   * we are explicitly setting the aliases to a value. In that case, we should actually overwrite
   * the stored aliases (including the display name) and use the specified aliases.
   *
   * @throws Exception on failure
   */
  @Test
  public void testUpdateFieldDisplayNameAndAliases() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    TestCreateOrg.createOrg(store, org);
    String metric = "metricname";
    TestCreateMetric.createMetric(store, org, metric);
    String field = "field", type = "STRING";
    TestAddField.createField(store, org, metric, field, type);

    String newDisplayName = "newfield";
    String[] aliases = new String[]{"alias1"};
    UpdateFieldRequest request = new UpdateFieldRequest()
      .setNewDisplayName(newDisplayName)
      .setAliases(aliases)
      .setFieldName(field)
      .setMetricName(metric);
    ReadMetricResponse read = writeAndReadMetric(store, org, request);
    ReadFieldResponse fieldResponse = getFieldWithName(read, newDisplayName);
    assertEquals(newDisplayName, fieldResponse.getName());
    assertArrayEquals(aliases, fieldResponse.getAliases());
  }

  private static ReadFieldResponse getFieldWithName(ReadMetricResponse response, String name) {
    return Arrays.asList(response.getFields())
                 .stream()
                 .filter(f -> f.getName().equals(name))
                 .findAny()
                 .orElseThrow(() -> new IllegalStateException("Field:" + name + " not found!"));
  }

  private static ReadMetricResponse writeAndReadMetric(SchemaStore store, String org,
    UpdateFieldRequest body)
    throws Exception {
    updateField(store, org, body);
    return TestReadMetric.read(store, org, body.getMetricName());
  }

  public static void updateField(SchemaStore store, String org, String metric, String field,
    String... aliases) throws Exception {
    UpdateFieldRequest body = new UpdateFieldRequest();
    body.setMetricName(metric);
    body.setFieldName(field);
    body.setAliases(aliases);
    updateField(store, org, body);
  }

  public static void updateField(SchemaStore store, String org, UpdateFieldRequest body)
    throws Exception {
    UpdateFieldRequestInternal request = new UpdateFieldRequestInternal();
    request.setOrgId(org);
    request.setBody(body);
    UpdateFieldHandler handler = handler(() -> new StoreManager(store));
    handler.handle(request, null);
  }

  private static UpdateFieldHandler handler(Provider<StoreManager> provider) {
    return new UpdateFieldHandler(provider, new UpdateRetryer(), 1);
  }
}
