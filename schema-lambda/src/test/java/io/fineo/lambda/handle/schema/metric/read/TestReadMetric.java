package io.fineo.lambda.handle.schema.metric.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.field.read.ReadFieldResponse;
import io.fineo.lambda.handle.schema.field.update.TestUpdateField;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.lambda.handle.schema.metric.field.TestAddField;
import io.fineo.lambda.handle.schema.metric.update.TestUpdateMetric;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.timestamp.MultiPatternTimestampParser;
import org.junit.Test;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.UUID;

import static io.fineo.lambda.handle.schema.field.read.TestReadField.field;
import static io.fineo.lambda.handle.schema.field.read.TestReadField.sort;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class TestReadMetric {

  @Test
  public void testNoFieldsOrAliasesRead() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    ReadMetricResponse response = read(store, org, metric);

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric current = clerk.getMetricForUserNameOrAlias(metric);
    assertEquals(current.getUserName(), response.getName());
    assertArrayEquals(new String[0], response.getAliases());
    assertArrayEquals(new ReadFieldResponse[]{ts()}, response.getFields());
  }

  private ReadFieldResponse ts() {
    return field(AvroSchemaProperties.TIMESTAMP_KEY, "LONG");
  }

  @Test
  public void testNoFieldsRead() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    String alias = "a1", alias2 = "a2";
    TestUpdateMetric.updateMetricAliases(store, org, metric, alias, alias2);

    ReadMetricResponse response = read(store, org, metric);

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric current = clerk.getMetricForUserNameOrAlias(metric);
    assertEquals(current.getUserName(), response.getName());
    assertArrayEquals(new String[]{alias, alias2}, response.getAliases());
    assertArrayEquals(new ReadFieldResponse[]{ts()}, response.getFields());
  }

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

    ReadMetricResponse response = read(store, org, metric);

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric current = clerk.getMetricForUserNameOrAlias(metric);
    assertEquals(current.getUserName(), response.getName());
    assertArrayEquals(new String[]{alias, alias2}, response.getAliases());
    assertArrayEquals(new ReadFieldResponse[]{field(f1, f1Type), field(f2, f2Type, f2Alias), ts()},
      sort(response.getFields()));
  }

  @Test
  public void testReadTimestampWithAlias() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);

    // update the timestamp alias
    TestUpdateField.updateField(store, org, metric, AvroSchemaProperties.TIMESTAMP_KEY, "ts");

    ReadMetricResponse response = read(store, org, metric);

    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric current = clerk.getMetricForUserNameOrAlias(metric);
    assertEquals(current.getUserName(), response.getName());
    assertArrayEquals(new String[0], response.getAliases());
    assertArrayEquals(new ReadFieldResponse[]{field(AvroSchemaProperties.TIMESTAMP_KEY, "LONG",
      "ts")}, response.getFields());
  }

  @Test
  public void testMissingParameters() throws Exception {
    ReadMetricRequest request = new ReadMetricRequest();
    HandlerTestUtils.failNoValueWithProvider(TestReadMetric::handleProvider, request);

    request.setOrgId("org");
    HandlerTestUtils.failNoValueWithProvider(TestReadMetric::handleProvider, request);
  }

  @Test
  public void testMetricDoesNotExist() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org";
    TestCreateOrg.createOrg(store, org);
    ReadMetricRequest request = new ReadMetricRequest();
    request.setOrgId(org);
    request.setMetricName("metric");

    try {
      Context context = Mockito.mock(Context.class);
      Mockito.when(context.getAwsRequestId()).thenReturn(UUID.randomUUID().toString());
      handler(store).handle(request, context);
    } catch (Exception e) {
      HandlerTestUtils.expectError(e, 404, "Not Found");
    }
  }

  @Test
  public void testReadTimestampPatterns() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org", metric = "metric";
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
    String t1 = MultiPatternTimestampParser.TimeFormats.RFC_1123_DATE_TIME.name();
    TestUpdateMetric.setTimestampPatterns(store, org, metric, t1);

    ReadMetricResponse response = read(store, org, metric);
    assertArrayEquals(new String[]{t1}, response.getTimestampPatterns());

    // set a new pattern
    String t2 = MultiPatternTimestampParser.TimeFormats.ISO_DATE_TIME.name();
    TestUpdateMetric.setTimestampPatterns(store, org, metric, t2);
    response = read(store, org, metric);
    assertArrayEquals(new String[]{t2}, response.getTimestampPatterns());

    // set multiple patterns
    TestUpdateMetric.setTimestampPatterns(store, org, metric, t1, t2);
    response = read(store, org, metric);
    assertArrayEquals(
      "Arrays didn't match. Order is important here - it defines the order that patterns are "
      + "evaluated.",
      new String[]{t1, t2}, response.getTimestampPatterns());
  }

  private ReadMetricResponse read(SchemaStore store, String org, String metric) throws Exception {
    ReadMetricRequest request = new ReadMetricRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    ReadMetricHandler handler = handler(store);
    return handler.handle(request, null);
  }

  private static ReadMetricHandler handleProvider(Provider<SchemaStore> store) {
    return new ReadMetricHandler(store);
  }

  private static ReadMetricHandler handler(SchemaStore store) {
    return handleProvider(() -> store);
  }
}
