package io.fineo.schema.store;

import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.schema.OldSchemaException;
import org.junit.Ignore;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Managing schema evolution for an organization with a 'real' {@link SchemaStore}.
 */
public class TestSchemaManagement {

  private static final String DEFAULT_METRIC_USER_NAME = "user-metric-name";
  private static final String DEFAULT_ORG_ID = "org1";

  /**
   * Add a field to a metric and then adding the same field again to the old schema will fail -
   * the schema has changed
   *
   * @throws Exception on failure
   */
  @Test
  public void testAddMetricField() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaBuilder.Organization org = createNewOrg();
    store.createNewOrganization(org);

    // add a new field to the metric
    String orgId = org.getMetadata().getMetadata().getCanonicalName();
    String metricId = org.getSchemas().keySet().iterator().next();
    OrgMetadata metadata = store.getOrgMetadata(orgId);
    Metric old = store.getMetricMetadata(orgId, metricId);
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaBuilder.OrganizationBuilder orgBuilder = builder.updateOrg(metadata);
    org = addOtherBooleanField(orgBuilder.updateSchema(old)).build().build();
    store.updateOrgMetric(org, old);

    // create a new schema with the old metric
    org = addOtherBooleanField(builder.updateOrg(metadata).updateSchema(old)).build().build();
    try {
      store.updateOrgMetric(org, old);
    } catch (OldSchemaException e) {
      // expected
    }
  }

  @Test
  public void testCreateNewMetricInExistingOrg() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    // do the first create
    SchemaBuilder.Organization org = createNewOrg();
    store.createNewOrganization(org);
    String oldMetricId = org.getSchemas().keySet().iterator().next();

    // create a new metric
    String orgId = org.getMetadata().getMetadata().getCanonicalName();
    OrgMetadata metadata = store.getOrgMetadata(orgId);
    SchemaBuilder builder = SchemaBuilder.create();
    org = builder.updateOrg(metadata).newMetric()
                 .withName("another metric type").withBytes("some bytes")
                 .asField()
                 .build().build();
    String newMetricId = org.getSchemas().keySet().iterator().next();
    assertNotEquals(newMetricId, oldMetricId);
    store.addNewMetricsInOrg(org);
    metadata = store.getOrgMetadata(orgId);
    Set<String> metricIds = metadata.getMetrics().keySet();
    assertEquals("Wrong number of metrics. Got metadata: " + metadata, 2, metricIds.size());
    assertTrue("Metrics don't contain " + oldMetricId, metricIds.contains(oldMetricId));
    assertTrue("Metrics don't contain " + newMetricId, metricIds.contains(newMetricId));
  }

  @Test
  public void testGetMetricFromAlias() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaBuilder.Organization org = createNewOrg();
    store.createNewOrganization(org);
    Metric metric = store.getMetricMetadataFromAlias(org.getMetadata(), DEFAULT_METRIC_USER_NAME);
    assertEquals(org.getSchemas().values().iterator().next(), metric);
  }

  @Test
  public void testGetNonExistentOrg() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    assertNull(store.getOrgMetadata("some org"));
  }

  @Test
  public void testGetNonExistentMetric() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaBuilder builder = SchemaBuilder.create();
    store.createNewOrganization(builder.newOrg(DEFAULT_ORG_ID)
                                       .newMetric().withName(DEFAULT_METRIC_USER_NAME).build()
                                       .build());
    OrgMetadata metadata = store.getOrgMetadata(DEFAULT_ORG_ID);
    assertNull(store.getMetricMetadataFromAlias(metadata, "other metric"));
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleRegisterOrganization() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaTestUtils.addNewOrg(store, DEFAULT_ORG_ID, DEFAULT_METRIC_USER_NAME, "field1");
    SchemaTestUtils.addNewOrg(store, DEFAULT_ORG_ID, DEFAULT_METRIC_USER_NAME, "field2");
  }

  @Test
  public void testOrgVersioning() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaBuilder.Organization org = SchemaTestUtils
      .addNewOrg(store, DEFAULT_METRIC_USER_NAME, DEFAULT_METRIC_USER_NAME, "field1");
    assertEquals("Org doesn't have default value on create", "0",
      org.getMetadata().getMetadata().getVersion());
    assertEquals("Didn't set org version on retrieval", "0",
      getOrgMetadata(store, org).getMetadata().getVersion());
    store.addNewMetricsInOrg(
      SchemaBuilder.create().updateOrg(org.getMetadata()).newMetric().withName("two").build()
                   .build());
    assertEquals("Didn't set org version on retrial", "1",
      getOrgMetadata(store, org).getMetadata().getVersion());
  }

  private OrgMetadata getOrgMetadata(SchemaStore store, SchemaBuilder.Organization org) {
    return store.getOrgMetadata(org.getMetadata().getMetadata().getCanonicalName());
  }

  @Test
  public void testMetricVersioning() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaBuilder.Organization org = SchemaTestUtils
      .addNewOrg(store, DEFAULT_METRIC_USER_NAME, DEFAULT_METRIC_USER_NAME, "field1");
    OrgMetadata metadata = getOrgMetadata(store, org);
    Metric metric = store.getMetricMetadataFromAlias(metadata, DEFAULT_METRIC_USER_NAME);
    assertEquals("Initial org metadata version incorrect", "0",
      metadata.getMetadata().getVersion());
    assertEquals("Initial metric metadata version incorrect", "0",
      metric.getMetadata().getMeta().getVersion());
    org = SchemaBuilder.create().updateOrg(metadata)
                       .updateSchema(metric)
                       .withBoolean("another boolean").asField()
                       .build().build();
    Metric next = org.getSchemas().values().iterator().next();
    assertEquals("Wrong updated metric version", Integer.toString(1),
      next.getMetadata().getMeta().getVersion());

    // do the update
    store.updateOrgMetric(org, metric);

    // check that the stored metadata changes as expected
    assertEquals("Org metadata didn't change, but got a schema version change!",
      "0", getOrgMetadata(store, org).getMetadata().getVersion());
    assertEquals("1", store.getMetricMetadata(org.getMetadata().getMetadata().getCanonicalName(),
      metric.getMetadata().getMeta().getCanonicalName()).getMetadata().getMeta().getVersion());
  }

  @Test
  @Ignore("This test fails - its part of why you should use StoreManager instead")
  public void testChangeMetricAliases() throws Exception {
    SchemaBuilder builder = SchemaBuilder.create();
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String fieldName = "f1";
    SchemaBuilder.Organization organization =
      SchemaTestUtils.addNewOrg(store, DEFAULT_ORG_ID, DEFAULT_METRIC_USER_NAME, fieldName);
    Metric metric = organization.getSchemas().values().iterator().next();
    String metricAlias = "metricAlias";

    organization = builder.updateOrg(organization.getMetadata())
                          .updateSchema(metric)
                          .withName(metricAlias)
                          .build().build();
    store.updateOrgMetric(organization, metric);

    // get the current metadata
    OrgMetadata updatedMetadata = store.getOrgMetadata(DEFAULT_ORG_ID);
    assertNotNull(
      store.getMetricMetadataFromAlias(updatedMetadata, DEFAULT_METRIC_USER_NAME));
    assertNotNull(store.getMetricMetadataFromAlias(updatedMetadata, metricAlias));
  }

  private SchemaBuilder.Organization createNewOrg() throws IOException {
    SchemaBuilder builder = SchemaBuilder.create();
    return createNewOrg(builder);
  }

  private SchemaBuilder.Organization createNewOrg(SchemaBuilder builder) throws IOException {
    SchemaBuilder.OrganizationBuilder orgBuilder = builder.newOrg(DEFAULT_ORG_ID);
    return orgBuilder.newMetric()
                     .withName(DEFAULT_METRIC_USER_NAME)
                     .withBoolean("field1").asField()
                     .build().build();
  }

  private SchemaBuilder.MetricBuilder addOtherBooleanField(
    SchemaBuilder.MetricBuilder metadata) {
    return metadata.withBoolean("other field").asField()
                   .withName("other user name");
  }
}
