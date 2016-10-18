package io.fineo.schema.store;

import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.schema.OldSchemaException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Copier to enable copying org schemas between schema stores
 */
public class StoreCopier {

  private final SchemaStore from;
  private final SchemaStore to;

  public StoreCopier(SchemaStore from, SchemaStore to) {
    this.from = from;
    this.to = to;
  }

  public void copy(String org) throws IOException, OldSchemaException {
    OrgMetadata orgInfo = from.getOrgMetadata(org);
    Map<String, Metric> metrics = new HashMap<>();
    for (String metricKey : orgInfo.getMetrics().keySet()) {
      Metric toMetric = from.getMetricMetadata(org, metricKey);
      metrics.put(metricKey, toMetric);
    }
    SchemaBuilder.Organization toOrg = new SchemaBuilder.Organization(orgInfo, metrics);
    to.createNewOrganization(toOrg);
  }
}
