package io.fineo.schema;

import io.fineo.internal.customer.metric.MetricMetadata;
import io.fineo.internal.customer.metric.OrganizationMetadata;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.Repository;

/**
 * Stores and retrives schema for record instances
 */
public class SchemaStore {
  private final Repository repo;

  public SchemaStore(Repository repo) {
    this.repo = repo;
  }

  public void createNewOrganization(OrganizationMetadata meta) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public OrganizationMetadata getSchemaTypes(String orgid) {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public MetricMetadata getMetricMetadata(String metricName) {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
