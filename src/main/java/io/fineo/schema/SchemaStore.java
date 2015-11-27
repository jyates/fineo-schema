package io.fineo.schema;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.metric.MetricMetadata;
import io.fineo.internal.customer.metric.OrganizationMetadata;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.schemarepo.Repository;
import org.schemarepo.SchemaEntry;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;

import java.io.IOException;
import java.util.List;

/**
 * Stores and retrives schema for record instances
 */
public class SchemaStore {
  private static final Log LOG = LogFactory.getLog(SchemaStore.class);
  private final Repository repo;

  public SchemaStore(Repository repo) {
    this.repo = repo;
  }

  public void createNewOrganization(SchemaBuilder.Organization organization)
    throws IllegalArgumentException, OldSchemaException {
    OrganizationMetadata meta = organization.getMetadata();
    Subject orgMetadata = repo.register(String.valueOf(meta.getOrgId()), null);
    try {
      orgMetadata.registerIfLatest(meta.toString(), null);
    } catch (SchemaValidationException e) {
      throw new IllegalArgumentException("Already have a schema for the organization", e);
    }
    // register the metrics below the repo
    for (MetricMetadata metric : organization.getSchemas()) {
      registerMetricInternal(meta.getOrgId(), metric, null);
    }
  }

  public void registerOrganizationSchema(String orgId, MetricMetadata schema,
    MetricMetadata previous) throws IllegalArgumentException, OldSchemaException {
    Subject subject = repo.lookup(orgId);
    Preconditions
      .checkArgument(subject != null, "Organization[%s] was not previously registered!", orgId);
    SchemaEntry entry = subject.latest();
    OrganizationMetadata metadata = parse(entry, OrganizationMetadata.getClassSchema());
    // ensure that the organization metadata has the schema's name as a field
    registerSchemaWithMetadata(subject, schema, metadata, entry);

    // register the metric itself
    registerMetricInternal(orgId, schema, previous);
  }

  private void registerMetricInternal(CharSequence orgId, MetricMetadata schema,
    MetricMetadata previous) throws IllegalArgumentException, OldSchemaException {
    Subject metricSubject = getMetricSubject(orgId, schema.getCannonicalname());
    if (metricSubject == null) {
      metricSubject = repo.register(getMetricSubjectName(orgId, schema.getCannonicalname()), null);
    }
    SchemaEntry latest = metricSubject.latest();
    MetricMetadata storedPrevious = parse(latest, MetricMetadata.getClassSchema());
    // register because its the latest
    if ((latest == null && previous == null) || storedPrevious.equals(previous)) {
      try {
        // register the schema as long as we are still the latest. Returns null if its changed,
        // in which case we fall through to the oldSchema exception
        if (metricSubject.registerIfLatest(schema.toString(), latest) != null) {
          return;
        }
      } catch (SchemaValidationException e) {
        throw new IllegalArgumentException(e);
      }
    }

    throw new OldSchemaException();

  }

  /**
   * Register a schema instance (a metric type) with the schema repository under the
   * organization's metadata. Does <b>not</b> register the schema itself; to register the schema
   * itself use {@link #registerMetricInternal(CharSequence, MetricMetadata, MetricMetadata)}
   */
  private void registerSchemaWithMetadata(Subject subject, MetricMetadata schema,
    OrganizationMetadata metadata, SchemaEntry latest) {
    // check that the metadata has the schema name. If not, add it and update
    if (!metadata.getFieldTypes().contains(schema.getCannonicalname())) {
      List<CharSequence> types = metadata.getFieldTypes();
      types.add(schema.getCannonicalname());
      metadata.setFieldTypes(types);
      try {
        subject.registerIfLatest(metadata.toString(), latest);
      } catch (SchemaValidationException e) {
        latest = subject.latest();
        LOG.info("Organization was updated, trying again to add " + schema + " to " + latest);
        registerSchemaWithMetadata(subject, schema, parse(latest, MetricMetadata.getClassSchema()),
          latest);
      }
    }
  }

  public OrganizationMetadata getSchemaTypes(CharSequence orgid) {
    Subject subject = repo.lookup(String.valueOf(orgid));
    if (subject == null) {
      return null;
    }

    return parse(subject.latest(), OrganizationMetadata.getClassSchema());
  }

  public MetricMetadata getMetricMetadata(CharSequence orgId, String metricName) {
    Subject subject = getMetricSubject(orgId, metricName);
    return parse(subject.latest(), MetricMetadata.getClassSchema());
  }

  private Subject getMetricSubject(CharSequence orgId, CharSequence metricName) {
    String subjectName = getMetricSubjectName(orgId, metricName);
    return repo.lookup(subjectName);
  }

  private String getMetricSubjectName(CharSequence orgId, CharSequence metricName) {
    return orgId + "." + metricName;
  }

  /**
   * Parse an Avro-encoded instance from the {@link SchemaEntry}, based on the specified schema.
   *
   * @param entry  entry to parse, can be <tt>null</tt>
   * @param schema schema to use when parsing, <b>cannot</b> be null.
   * @return the parsed schema entry or <tt>null</tt> if the entry was null
   * @throws IllegalArgumentException if the object could not be parsed
   */
  private <T> T parse(SchemaEntry entry, Schema schema) throws IllegalArgumentException {
    if (entry == null) {
      return null;
    }
    try {
      SpecificDatumReader reader = new SpecificDatumReader(schema);
      return (T) reader.read(null,
        DecoderFactory.get().jsonDecoder(schema, entry.getSchema()));
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse organization schema!", e);
    }
  }
}
