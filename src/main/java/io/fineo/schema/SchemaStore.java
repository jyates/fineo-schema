package io.fineo.schema;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.schemarepo.Repository;
import org.schemarepo.SchemaEntry;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stores and retrieves schema for record instances
 */
public class SchemaStore {
  private static final Log LOG = LogFactory.getLog(SchemaStore.class);
  private final Repository repo;

  public SchemaStore(Repository repo) {
    this.repo = repo;
  }

  public void createNewOrganization(SchemaBuilder.Organization organization)
    throws IllegalArgumentException, OldSchemaException {
    Metadata meta = organization.getMetadata();
    Subject orgMetadata = repo.register(String.valueOf(meta.getCanonicalName()), null);
    try {
      orgMetadata.registerIfLatest(meta.toString(), null);
    } catch (SchemaValidationException e) {
      throw new IllegalArgumentException("Already have a schema for the organization", e);
    }
    // register the metrics below the repo
    for (Metric metric : organization.getSchemas()) {
      registerMetricInternal(meta.getCanonicalName(), metric, null);
    }
  }

  public void registerOrganizationSchema(CharSequence orgId, Metric schema,
    Metric previous) throws IllegalArgumentException, OldSchemaException {
    Subject subject = repo.lookup(String.valueOf(orgId));
    Preconditions
      .checkArgument(subject != null, "Organization[%s] was not previously registered!", orgId);
    SchemaEntry entry = subject.latest();
    Metadata metadata = parse(entry, Metadata.getClassSchema());
    // ensure that the organization metadata has the schema's name as a field
    registerOrgSchemaWithMetadata(subject, schema, metadata, entry);

    // register the metric itself
    registerMetricInternal(orgId, schema, previous);
  }

  private void registerMetricInternal(CharSequence orgId, Metric schema,
    Metric previous) throws IllegalArgumentException, OldSchemaException {
    Subject metricSubject = getMetricSubject(orgId, schema.getMetadata().getCanonicalName());
    if (metricSubject == null) {
      metricSubject = repo.register(
        getMetricSubjectName(orgId, String.valueOf(schema.getMetadata().getCanonicalName())),
        null);
    }
    SchemaEntry latest = metricSubject.latest();
    Metric storedPrevious = parse(latest, Metric.getClassSchema());
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
   * itself use {@link #registerMetricInternal(CharSequence, Metric, Metric)}
   */
  private void registerOrgSchemaWithMetadata(Subject subject, Metric schema,
    Metadata orgMetadata, SchemaEntry latest) {
    // check that the metadata has the schema name. If not, add it and update
    Map<String, List<String>> metrics =
      orgMetadata.getMetricTypes().getCanonicalNamesToAliases();
    if (!metrics.containsKey(schema.getMetadata().getCanonicalName())) {
      return;
    }
    metrics.put(schema.getMetadata().getCanonicalName(), new ArrayList<>(0));
    try {
      subject.registerIfLatest(SchemaUtils.toString(orgMetadata), latest);
    } catch (IOException | SchemaValidationException e) {
      latest = subject.latest();
      LOG.info("Organization was updated, trying again to add " + schema + " to " + latest);
      registerOrgSchemaWithMetadata(subject, schema,
        parse(latest, Metric.getClassSchema()),
        latest);
    }
  }

  public Metadata getSchemaTypes(String orgid) {
    Subject subject = repo.lookup(orgid);
    if (subject == null) {
      return null;
    }

    return parse(subject.latest(), Metadata.getClassSchema());
  }

  public Metric getMetricMetadata(CharSequence orgId, String metricName) {
    Subject subject = getMetricSubject(orgId, metricName);
    return parse(subject.latest(), Metric.getClassSchema());
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
      return SchemaUtils.readFromString(entry.getSchema(), schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse organization schema!", e);
    }
  }
}
