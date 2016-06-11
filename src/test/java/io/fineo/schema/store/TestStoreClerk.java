package io.fineo.schema.store;

import io.fineo.internal.customer.Metric;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.SchemaTestUtils;
import org.apache.avro.Schema;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class TestStoreClerk {
  private static final Logger LOG = LoggerFactory.getLogger(TestStoreClerk.class);

  @Test
  public void testOneMetric() throws Exception {
    verifyFieldsAndMetrics("orgid", "metricid", "field1");
  }

  @Test
  public void testMultipleMetrics() throws Exception {
    verifyFieldsAndMetrics("orgid", "metricid", "field1", "field2", "n3");
  }

  @Test
  public void testMultipleMetricNames() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String orgId = "org1", metric = "metricid", fieldName = "field1";
    SchemaBuilder.Organization organization =
      SchemaTestUtils.addNewOrg(store, orgId, metric, fieldName);
    StoreClerk helper = new StoreClerk(store, orgId);
    StoreClerk.Metric sm = helper.getMetrics().get(0);
    Metric m = organization.getSchemas().values().iterator().next();
    String fieldCname = sm.getCanonicalFieldNames().get(0);

    SchemaBuilder builder = SchemaBuilder.create();
    try {
      SchemaBuilder.OrganizationBuilder orgBuilder =
        builder.updateOrg(organization.getMetadata())
               .updateSchema(m)
               .updateField(fieldCname).withAlias("other alias").asField()
               .build();
      organization = orgBuilder.build();
    } catch (IndexOutOfBoundsException e) {
      LOG.info("Got metadata:" + m);
      LOG.info("Using field cname: " + fieldCname);
      throw e;
    }
    store.updateOrgMetric(organization, m);

    List<StoreClerk.Metric> metrics = helper.getMetrics();
    assertEquals(1, metrics.size());
    StoreClerk.Metric mh = metrics.get(0);
    assertUserBooleanFields(mh, fieldName);
  }

  @Test
  public void testStoreHelperMetric() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String orgId = "org1", metric = "metricid", fieldName = "field1";
    SchemaBuilder.Organization organization =
      SchemaTestUtils.addNewOrg(store, orgId, metric, fieldName);
    StoreClerk helper = new StoreClerk(store, orgId);
    StoreClerk.Metric sm = helper.getMetrics().get(0);
    Metric m = organization.getSchemas().values().iterator().next();

    String fieldCname = sm.getCanonicalFieldNames().get(0);
    assertEquals(fieldName, sm.getUserFieldNameFromCanonicalName(fieldCname));
    assertEquals(fieldCname, sm.getCanonicalNameFromUserFieldName(fieldName));
    assertEquals(orgId, sm.getOrgId());
    assertEquals(m.getMetadata().getCanonicalName(), sm.getMetricId());
    assertEquals(metric, sm.getUserName());
  }

  @Test
  public void testAccessMetricByAlias() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String orgId = "org1", metric = "metricid", fieldName = "field1";
    SchemaBuilder.Organization organization =
      SchemaTestUtils.addNewOrg(store, orgId, metric, fieldName);

    String metricAlias = "metricAlias";
    SchemaBuilder builder = SchemaBuilder.create();
    Metric old = organization.getSchemas().values().iterator().next();
    organization = builder.updateOrg(organization.getMetadata())
                          .updateSchema(old)
                          .withName(metricAlias)
                          .build().build();
    store.updateOrgMetric(organization, old);

    StoreClerk sh = new StoreClerk(store, orgId);
    StoreClerk.Metric mh = sh.getMetricForUserFieldName(metricAlias);
    assertEquals(metricAlias, mh.getUserName());
    assertEquals(old.getMetadata().getCanonicalName(), mh.getMetricId());
    assertEquals(orgId, mh.getOrgId());
  }

  private void verifyFieldsAndMetrics(String org, String metric, String... fields)
    throws IOException, OldSchemaException {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaTestUtils.addNewOrg(store, org, metric, fields);

    StoreClerk helper = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = helper.getMetrics();
    assertEquals(1, metrics.size());
    StoreClerk.Metric metricType = metrics.get(0);
    assertEquals(metric, metricType.getUserName());

    assertUserBooleanFields(metricType, fields);
  }

  private void assertUserBooleanFields(StoreClerk.Metric metricType, String... fields) {
    List<Pair<String, Schema.Type>> userFields = metricType.getUserVisibleFields();
    List<Pair<String, Schema.Type>> expected = newArrayList(fields).stream()
                                                                   .map(f -> new Pair<>(f,
                                                                     Schema.Type.BOOLEAN))
                                                                   .collect(toList());
    Comparator<Pair<String, Schema.Type>> comp = (o1, o2) -> o1.getKey().compareTo(o2.getKey());
    Collections.sort(userFields, comp);
    Collections.sort(expected, comp);
    assertEquals(expected, userFields);
  }
}
