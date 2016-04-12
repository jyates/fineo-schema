package io.fineo.schema;

import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.SchemaTestUtils;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
    String orgId = org.getMetadata().getCanonicalName();
    String metricId = org.getSchemas().keySet().iterator().next();
    Metadata metadata = store.getOrgMetadata(orgId);
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
    String orgId = org.getMetadata().getCanonicalName();
    Metadata metadata = store.getOrgMetadata(orgId);
    SchemaBuilder builder = SchemaBuilder.create();
    org = builder.updateOrg(metadata).newSchema()
                 .withName("another metric type").withBytes("some bytes")
                 .asField()
                 .build().build();
    String newMetricId = org.getSchemas().keySet().iterator().next();
    assertNotEquals(newMetricId, oldMetricId);
    store.addNewMetricsInOrg(org);
    metadata = store.getOrgMetadata(orgId);
    Set<String> metricIds = metadata.getCanonicalNamesToAliases().keySet();
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
                                       .newSchema().withName(DEFAULT_METRIC_USER_NAME).build()
                                       .build());
    Metadata metadata = store.getOrgMetadata(DEFAULT_ORG_ID);
    assertNull(store.getMetricMetadataFromAlias(metadata, "other metric"));
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleRegisterOrganization() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    SchemaTestUtils.addNewOrg(store, DEFAULT_ORG_ID, DEFAULT_METRIC_USER_NAME, "field1");
    SchemaTestUtils.addNewOrg(store, DEFAULT_ORG_ID, DEFAULT_METRIC_USER_NAME, "field2");
  }

  private SchemaBuilder.Organization createNewOrg() throws IOException {
    SchemaBuilder builder = SchemaBuilder.create();
    return createNewOrg(builder);
  }

  private SchemaBuilder.Organization createNewOrg(SchemaBuilder builder) throws IOException {
    SchemaBuilder.OrganizationBuilder orgBuilder = builder.newOrg(DEFAULT_ORG_ID);
    return orgBuilder.newSchema()
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
