package io.fineo.lambda.handle.schema.field.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.lambda.handle.schema.metric.field.TestAddField;
import io.fineo.lambda.handle.schema.metric.update.TestUpdateMetric;
import io.fineo.schema.store.SchemaStore;
import org.junit.Test;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;


public class TestReadField {

  @Test
  public void testRead() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    // update the alias
    String alias = "a1", alias2 = "a2";
    TestUpdateMetric.updateMetricAliases(store, org, metric, alias, alias2);

    // add some fields
    String f1 = "f1", f1Type = "STRING", f2 = "f2", f2Type = "INT", f2Alias = "f2_alias";
    TestAddField.createField(store, org, metric, f1, f1Type);
    TestAddField.createField(store, org, metric, f2, f2Type, f2Alias);

    verifyReadField(store, org, metric, f1, f1Type);
    verifyReadField(store, org, metric, f2, f2Type, f2Alias);
  }

  private static void verifyReadField(SchemaStore store, String org, String metric, String field,
    String type, String... aliases) throws Exception {
    ReadFieldRequest request = new ReadFieldRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    request.setFieldName(field);
    ReadFieldHandler handler = handler(store);
    assertEquals(field(field, type, aliases), handler.handle(request, null));
  }

  @Test
  public void testMissingParameters() throws Exception {
    ReadFieldRequest request = new ReadFieldRequest();
    HandlerTestUtils.failNoValueWithProvider(TestReadField::handleProvider, request);

    request.setOrgId("org");
    HandlerTestUtils.failNoValueWithProvider(TestReadField::handleProvider, request);

    request.setMetricName("metric");
    HandlerTestUtils.failNoValueWithProvider(TestReadField::handleProvider, request);
  }

  @Test
  public void testFieldDoesNotExist() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
    ReadFieldRequest request = new ReadFieldRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    request.setFieldName("field");

    try {
      Context context = Mockito.mock(Context.class);
      Mockito.when(context.getAwsRequestId()).thenReturn(UUID.randomUUID().toString());
      handler(store).handle(request, context);
    } catch (Exception e) {
      HandlerTestUtils.expectError(e, 404, "Not Found");
    }
  }

  private static ReadFieldHandler handleProvider(Provider<SchemaStore> store) {
    return new ReadFieldHandler(store);
  }

  private static ReadFieldHandler handler(SchemaStore store) {
    return handleProvider(() -> store);
  }

  public static ReadFieldResponse[] sort(ReadFieldResponse[] fields) {
    if (fields == null || fields.length == 0) {
      return fields;
    }
    List<ReadFieldResponse> list = newArrayList(fields);
    Collections.sort(list, (r1, r2) -> r1.getName().compareTo(r2.getName()));
    return list.toArray(new ReadFieldResponse[0]);
  }

  public static ReadFieldResponse field(String name, String type, String... aliases) {
    ReadFieldResponse response = new ReadFieldResponse();
    response.setName(name);
    response.setType(type);
    response.setAliases(aliases);
    return response;
  }
}
