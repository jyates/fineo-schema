package io.fineo.schema.store;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.MetricMetadata;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.avro.SchemaNameUtils;
import org.apache.avro.Schema;
import org.schemarepo.Repository;
import org.schemarepo.SchemaEntry;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Stores and retrieves schema for record instances
 */
public class SchemaStore {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaStore.class);
  private final Repository repo;

  public SchemaStore(Repository repo) {
    this.repo = repo;
  }

  public void createNewOrganization(SchemaBuilder.Organization organization)
    throws IllegalArgumentException, OldSchemaException, IOException {
    OrgMetadata orgMetadata = organization.getMetadata();
    String orgID = orgMetadata.getMetadata().getCanonicalName();
    Subject subject = repo.register(orgID, null);
    try {
      SchemaEntry entry = subject.registerIfLatest(SchemaNameUtils.toString(orgMetadata), null);
      Preconditions.checkState(entry != null, "Have an existing schema for the organization!");
    } catch (SchemaValidationException e) {
      throw new IllegalArgumentException("Already have a schema for the organization", e);
    }
    // register the metrics below the repo
    for (Metric metric : organization.getSchemas().values()) {
      registerMetricInternal(orgID, metric, null);
    }
  }

  public void updateOrgMetric(OrgMetadata orgMetadata, Metric next,
    Metric previous) throws IllegalArgumentException, OldSchemaException, IOException {
    String orgId = orgMetadata.getMetadata().getCanonicalName();
    Subject orgSubject = repo.lookup(orgId);
    checkArgument(orgSubject != null, "Organization [%s] was not previously registered!", orgId);
    // ensure the metric gets registered
    registerSchemaIfMetricUnknown(orgSubject, orgMetadata, next);
    // register the metric itself
    registerMetricInternal(orgId, next, previous);
  }

  /**
   * Register an organization metric type. Does <b>not</b> register the schema itself; to
   * register the schema itself use {@link #registerMetricInternal(CharSequence, Metric, Metric)}.
   * <p>
   * If the organization already has the given schema returns without changing the metadata
   * </p>
   */
  private void registerSchemaIfMetricUnknown(Subject org, OrgMetadata orgMetadata, Metric next)
    throws IOException {
    SchemaEntry entry = org.latest();
    // check to see if we already know about this org
    OrgMetadata currentOrgMetadata = parse(entry, OrgMetadata.getClassSchema());
    Metadata metricBaseMetdata = next.getMetadata().getMeta();
    String metricId = metricBaseMetdata.getCanonicalName();
    if (currentOrgMetadata.getMetrics().containsKey(metricId)) {
      LOG.debug("Org already has metricID: " + metricId);
      return;
    }

    // update the org schema to what we just got sent
    try {
      setVersion(orgMetadata, entry);
      org.registerIfLatest(SchemaNameUtils.toString(orgMetadata), entry);
    } catch (SchemaValidationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void updateOrganization(OrgMetadata next) throws IOException {
    String orgId = next.getMetadata().getCanonicalName();
    Subject orgSubject = repo.lookup(orgId);
    SchemaEntry latestEntry = orgSubject.latest();
    try {
      latestEntry = orgSubject.registerIfLatest(SchemaNameUtils.toString(next), latestEntry);
    } catch (SchemaValidationException e) {
      throw new IllegalArgumentException(e);
    }
    setVersion(next, latestEntry);
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
    for (Map.Entry<String, Metric> metrics : org.getSchemas().entrySet()) {
      updateOrgMetric(org.getMetadata(), metrics.getValue(), null);
    }
  }

  public void updateOrgMetric(SchemaBuilder.Organization org, Metric old)
    throws IOException, OldSchemaException {
    // find the metric in the org
    String metricID = old == null ? null : old.getMetadata().getMeta().getCanonicalName();
    Metric updated = org.getSchemas().get(metricID);
    updateOrgMetric(org.getMetadata(), updated, old);
  }

  private void registerMetricInternal(CharSequence orgId, Metric schema,
    Metric previous) throws IllegalArgumentException, OldSchemaException, IOException {
    Subject metricSubject =
      getMetricSubject(orgId, schema.getMetadata().getMeta().getCanonicalName());
    if (metricSubject == null) {
      metricSubject = repo
        .register(getMetricSubjectName(orgId, schema.getMetadata().getMeta().getCanonicalName()),
          null);
    }
    SchemaEntry latest = metricSubject.latest();
    Metric storedPrevious = parse(latest, Metric.getClassSchema());
    // register because its the latest
    if ((latest == null && previous == null) || storedPrevious.equals(previous)) {
      try {
        // register the schema as long as we are still the latest. Returns null if its changed,
        // in which case we fall through to the oldSchema exception
        SchemaEntry entry =
          metricSubject.registerIfLatest(SchemaNameUtils.toString(schema), latest);
        if (entry != null) {
          setVersion(schema.getMetadata(), entry);
          return;
        }
      } catch (SchemaValidationException e) {
        throw new IllegalArgumentException(e);
      }
    }


    throw new OldSchemaException(storedPrevious, previous);
  }

  private void setVersion(OrgMetadata metadata, SchemaEntry entry) {
    setVersion(metadata.getMetadata(), entry);
  }

  private void setVersion(MetricMetadata metadata, SchemaEntry entry) {
    setVersion(metadata.getMeta(), entry);
  }

  private void setVersion(Metadata metadata, SchemaEntry entry) {
    if (entry != null) {
      metadata.setVersion(entry.getId());
    }
  }

  /**
   * @param orgId
   * @return the stored metadata for the org, if its present
   */
  public OrgMetadata getOrgMetadata(String orgId) {
    LOG.debug("Looking up org: {}", orgId);
    Subject subject = repo.lookup(orgId);
    if (subject == null) {
      return null;
    }
    LOG.debug("Got subject for org: {}", orgId);
    SchemaEntry entry = subject.latest();
    OrgMetadata metadata = parse(entry, OrgMetadata.getClassSchema());
    LOG.debug("Parsed org metadata: \n{}", metadata);
    setVersion(metadata, entry);
    LOG.debug("Set version to: {}", entry.getId());
    return metadata;
  }

  /**
   * Helper method for {@link #getMetricMetadata(CharSequence, String)}
   */
  public Metric getMetricMetadata(RecordMetadata meta) {
    return getMetricMetadata(meta.getOrgID(), meta.getMetricCanonicalType());
  }

  public String getMetricCNameFromAlias(OrgMetadata org, String aliasMetricName) {
    if (org.getMetrics() == null) {
      return null;
    }
    Optional<String> canonicalName =
      org.getMetrics().entrySet().stream()
         .filter(SchemaUtils.metricHasAlias(aliasMetricName))
         .map(entry -> entry.getKey())
         .findFirst();
    return canonicalName.orElse(null);
  }

  /**
   * Similar to {@link #getMetricMetadata(CharSequence, String)}, but you specify the an aliased
   * metric name. The canonical metric name is then looked up from the org ID and then used to
   * retrieve the metric information via {@link #getMetricMetadata(CharSequence, String)}
   *
   * @param aliasMetricName customer visible metric name
   * @return
   */
  public Metric getMetricMetadataFromAlias(OrgMetadata org, String aliasMetricName) {
    Preconditions.checkNotNull(org);
    // find the canonical name to match the alias we were given
    String canonicalName = getMetricCNameFromAlias(org, aliasMetricName);
    return canonicalName != null ?
           this.getMetricMetadata(org.getMetadata().getCanonicalName(), canonicalName) :
           null;
  }

  public OrgMetricMetadata getOrgMetricMetadataForAliasMetricName(OrgMetadata org,
    String aliasMetricName) {
    Optional<Map.Entry<String, OrgMetricMetadata>> match =
      org.getMetrics().entrySet().stream()
         .filter(SchemaUtils.metricHasAlias(aliasMetricName))
         .findFirst();
    return match.isPresent() ?
           match.get().getValue() :
           null;
  }

  /**
   * @param orgId
   * @param canonicalMetricName
   * @return metric information for the specific metric name under the organization
   */
  public Metric getMetricMetadata(CharSequence orgId, String canonicalMetricName) {
    Subject subject = getMetricSubject(orgId, canonicalMetricName);
    SchemaEntry entry = subject.latest();
    Metric metric = parse(entry, Metric.getClassSchema());
    setVersion(metric.getMetadata(), entry);
    return metric;
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

  /**
   * Update the organziation for the specified metrics. Expects the previous metadata to not be
   * <tt>null</tt>, since the org should already exist.
   *
   * @param org            updated organization to publish
   * @param updatedMetrics map of metric name and version to update
   * @param previous       metadata describing the previous organization
   */
  public void updateOrg(SchemaBuilder.Organization org, Map<String, Metric> updatedMetrics,
    OrgMetadata previous) throws IOException, OldSchemaException {
    Preconditions.checkNotNull(previous, "Don't have any previous metadata for the organization!");
    OrgMetadata current = org.getMetadata();
    // update the parent org metadata. Only time we don't want to do this is if we aren't
    // updating the map of the metrics AND we are not updating the alias names of the metric.
    // For right now, its just easier to update it, rather than trying to be smart about when it is
    // correct or not to do the lookup #startup
    updateOrganization(current);
    for (Map.Entry<String, Metric> previousMetric : updatedMetrics.entrySet()) {
      Metric next = org.getSchemas().get(previousMetric.getKey());
      registerMetricInternal(current.getMetadata().getCanonicalName(), next,
        previousMetric.getValue());
    }
  }
}
