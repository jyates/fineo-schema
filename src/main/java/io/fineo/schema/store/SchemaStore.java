package io.fineo.schema.store;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.avro.SchemaNameUtils;
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
import java.util.Optional;

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
    throws IllegalArgumentException, OldSchemaException, IOException {
    Metadata meta = organization.getMetadata();
    Subject orgMetadata = repo.register(meta.getCanonicalName(), null);
    try {
      SchemaEntry entry = orgMetadata.registerIfLatest(SchemaNameUtils.toString(meta), null);
      Preconditions.checkState(entry != null, "Have an existing schema for the organization!");
    } catch (SchemaValidationException e) {
      throw new IllegalArgumentException("Already have a schema for the organization", e);
    }
    // register the metrics below the repo
    for (Metric metric : organization.getSchemas().values()) {
      registerMetricInternal(meta.getCanonicalName(), metric, null);
    }
  }

  public void updateOrgMetric(CharSequence orgId, Metric next,
    Metric previous) throws IllegalArgumentException, OldSchemaException, IOException {
    Subject orgSubject = repo.lookup(String.valueOf(orgId));
    Preconditions
      .checkArgument(orgSubject != null, "Organization[%s] was not previously registered!", orgId);
    SchemaEntry entry = orgSubject.latest();
    Metadata metadata = parse(entry, Metadata.getClassSchema());
    // register the schema's name (if new) or ensure it has the schema name
    registerOrgSchemaWithMetadata(orgSubject, next, metadata, entry);

    // register the metric itself
    registerMetricInternal(orgId, next, previous);
  }

  /**
   * All the metrics in the {@link SchemaBuilder.Organization} to the org's schema. All the
   * metrics must be <b>new</b> or the first non-new metric will throw an {@link
   * OldSchemaException} and no further schemas will be updated.
   *
   * @param org to update
   * @throws IOException        if we have trouble communicating with the schema store
   * @throws OldSchemaException if one of the metrics already exists
   */
  public void addNewMetricsInOrg(SchemaBuilder.Organization org)
    throws IOException, OldSchemaException {
    String canonicalName = org.getMetadata().getCanonicalName();
    for (Map.Entry<String, Metric> metrics : org.getSchemas().entrySet()) {
      updateOrgMetric(canonicalName, metrics.getValue(), null);
    }
  }

  public void updateOrgMetric(SchemaBuilder.Organization org, Metric old)
    throws IOException, OldSchemaException {
    // find the metric in the org
    String metricID = old == null ? null : old.getMetadata().getCanonicalName();
    Metric updated = org.getSchemas().get(metricID);
    updateOrgMetric(org.getMetadata().getCanonicalName(), updated, old);
  }

  private void registerMetricInternal(CharSequence orgId, Metric schema,
    Metric previous) throws IllegalArgumentException, OldSchemaException, IOException {
    Subject metricSubject = getMetricSubject(orgId, schema.getMetadata().getCanonicalName());
    if (metricSubject == null) {
      metricSubject =
        repo.register(getMetricSubjectName(orgId, schema.getMetadata().getCanonicalName()), null);
    }
    SchemaEntry latest = metricSubject.latest();
    Metric storedPrevious = parse(latest, Metric.getClassSchema());
    // register because its the latest
    if ((latest == null && previous == null) || storedPrevious.equals(previous)) {
      try {
        // register the schema as long as we are still the latest. Returns null if its changed,
        // in which case we fall through to the oldSchema exception
        if (metricSubject.registerIfLatest(SchemaNameUtils.toString(schema), latest) != null) {
          return;
        }
      } catch (SchemaValidationException e) {
        throw new IllegalArgumentException(e);
      }
    }


    throw new OldSchemaException(storedPrevious, previous);
  }

  /**
   * Register an organization metric type. Does <b>not</b> register the schema itself; to
   * register the schema itself use {@link #registerMetricInternal(CharSequence, Metric, Metric)}.
   * <p>
   * If the organization already has the given schema returns without changing the metadata
   * </p>
   */
  private void registerOrgSchemaWithMetadata(Subject subject, Metric schema,
    Metadata orgMetadata, SchemaEntry latest) {
    // check that the metadata has the schema name. If not, add it and update
    Map<String, List<String>> metrics = orgMetadata.getCanonicalNamesToAliases();
    String metricId = schema.getMetadata().getCanonicalName();
    if (metrics.containsKey(metricId)) {
      LOG.debug("Org already has metricID: " + metricId);
      return;
    }
    try {
      subject.registerIfLatest(SchemaNameUtils.toString(orgMetadata), latest);
    } catch (IOException | SchemaValidationException e) {
      latest = subject.latest();
      LOG.info("Organization was updated, trying again to add " + schema + " to " + latest);
      registerOrgSchemaWithMetadata(subject, schema,
        parse(latest, Metric.getClassSchema()),
        latest);
    }
  }

  /**
   * @param orgId
   * @return the stored metadata for the org, if its present
   */
  public Metadata getOrgMetadata(String orgId) {
    Subject subject = repo.lookup(orgId);
    if (subject == null) {
      return null;
    }

    return parse(subject.latest(), Metadata.getClassSchema());
  }

  /**
   * Helper method for {@link #getMetricMetadata(CharSequence, String)}
   */
  public Metric getMetricMetadata(RecordMetadata meta) {
    return getMetricMetadata(meta.getOrgID(), meta.getMetricCanonicalType());
  }

  /**
   * Similar to {@link #getMetricMetadata(CharSequence, String)}, but you specify the an aliased
   * metric name. The canonical metric name is then looked up from the org ID and then used to
   * retrieve the metric information via {@link #getMetricMetadata(CharSequence, String)}
   *
   * @param aliasMetricName customer visible metric name
   * @return
   */
  public Metric getMetricMetadataFromAlias(Metadata org, String aliasMetricName) {
    Preconditions.checkNotNull(org);
    // find the canonical name to match the alias we were given
    Optional<String> canonicalName =
      org.getCanonicalNamesToAliases().entrySet().stream()
         .filter(entry -> entry.getValue().contains(aliasMetricName))
         .map(entry -> entry.getKey())
         .findFirst();
    return canonicalName.isPresent() ?
           this.getMetricMetadata(org.getCanonicalName(), canonicalName.get()) :
           null;
  }

  /**
   * @param orgId
   * @param canonicalMetricName
   * @return metric information for the specific metric name under the organization
   */
  public Metric getMetricMetadata(CharSequence orgId, String canonicalMetricName) {
    Subject subject = getMetricSubject(orgId, canonicalMetricName);
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
      return SchemaNameUtils.readFromString(entry.getSchema(), schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse organization schema!", e);
    }
  }
}
