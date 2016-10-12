package io.fineo.lambda.handle.schema.field.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Provider;
import io.fineo.client.model.schema.field.ReadFieldRequest;
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
import java.util.function.Consumer;

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
    ReadFieldRequestInternal request = new ReadFieldRequestInternal();
    request.setOrgId(org);
    ReadFieldRequest body = new ReadFieldRequest();
    body.setMetricName(metric);
    body.setFieldName(field);
    request.setBody(body);
    ReadFieldHandler handler = handler(store);
    assertEquals(field(field, type, aliases), handler.handle(request, null));
  }

  @Test
  public void testMissingParameters() throws Exception {
    ReadFieldRequestInternal request = verifyFailForRequest(null, r -> {
    });

    ReadFieldRequest body = new ReadFieldRequest();
    verifyFailForRequest(request, r -> r.setOrgId(""));
    verifyFailForRequest(request, r -> r.setOrgId("orgId"));
    verifyFailForRequest(request, r -> {
      body.setMetricName("");
      request.setBody(body);
    });
    verifyFailForRequest(request, r -> {
      body.setMetricName("");
      request.setBody(body);
    });
  }

  private ReadFieldRequestInternal verifyFailForRequest(ReadFieldRequestInternal request,
    Consumer<ReadFieldRequestInternal> update) throws Exception {
    if (request == null) {
      request = new ReadFieldRequestInternal();
    }
    update.accept(request);
    HandlerTestUtils.failNoValueWithProvider(TestReadField::handleProvider, request);
    return request;
  }

  @Test
  public void testFieldDoesNotExist() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
    ReadFieldRequestInternal request = new ReadFieldRequestInternal();
    request.setOrgId(org);
    ReadFieldRequest body = new ReadFieldRequest();
    body.setMetricName(metric);
    body.setFieldName("field");
    request.setBody(body);

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
