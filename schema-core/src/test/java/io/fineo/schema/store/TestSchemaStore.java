package io.fineo.schema.store;

import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetadata;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestSchemaStore {
  private static final String ORG_ID = "orgId";
  private static final String NEW_SCHEMA_DISPLAY_NAME = "newschema";
  private static final String BOOLEAN_FIELD_NAME = "bField";

  @Test
  public void testCopySchemaBetweenStores() throws Exception {
    SchemaBuilder.Organization org =
      SchemaBuilder.create().newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
                   .withBoolean(BOOLEAN_FIELD_NAME).asField().build().build();
    InMemoryRepository fromRepo = new InMemoryRepository(ValidatorFactory.EMPTY);
    InMemoryRepository toRepo = new InMemoryRepository(ValidatorFactory.EMPTY);
    // setup the org in the store
    SchemaStore from = new SchemaStore(fromRepo);
    from.createNewOrganization(org);

    SchemaStore to = new SchemaStore(toRepo);
    OrgMetadata orgInfo = from.getOrgMetadata(ORG_ID);
    Map<String, Metric> metrics = new HashMap<>();
    for (String metricKey : orgInfo.getMetrics().keySet()) {
      Metric toMetric = from.getMetricMetadata(ORG_ID, metricKey);
      metrics.put(metricKey, toMetric);
    }
    SchemaBuilder.Organization toOrg = new SchemaBuilder.Organization(orgInfo, metrics);
    to.createNewOrganization(toOrg);

    // make sure that the org/metric matches
    verifySchemasMatch(from, to, ORG_ID);
  }

  public static void verifySchemasMatch(SchemaStore store, SchemaStore store2, String org) {
    StoreClerk fromClerk = new StoreClerk(store, org);
    StoreClerk toClerk = new StoreClerk(store2, org);
    assertEquals(fromClerk.getOrgMetadataForTesting(), toClerk.getOrgMetadataForTesting());
    assertEquals(fromClerk.getMetrics().stream().map(m -> m.getUnderlyingMetric()).collect
      (Collectors.toList()), toClerk.getMetrics().stream().map(m -> m.getUnderlyingMetric()).collect
      (Collectors.toList()));
  }
}
