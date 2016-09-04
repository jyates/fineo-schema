package io.fineo.schema.store;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.avro.SchemaNameUtils;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Builder to generate a storable (in a {@link SchemaStore}) schema and organization/metric type
 * hierarchy. Can also be used to update existing schemas bound to a metric or organization.
 * <p>
 * This does <b>not support use of STOP WORDS</b> that - like all new dev - is incorporated in
 * {@link StoreManager}. It should only be used in the context of the {@link StoreManager} to do
 * the heavy lifting of building/updating the actual avro schema instance.
 * </p>
 */
class SchemaBuilder {

  public static SchemaBuilder create() {
    return new SchemaBuilder(SchemaNameGenerator.DEFAULT_INSTANCE);
  }

  public static SchemaBuilder createForTesting(SchemaNameGenerator gen) {
    return new SchemaBuilder(gen);
  }

  private SchemaBuilder(SchemaNameGenerator gen) {
    this.gen = gen;
  }

  private final SchemaNameGenerator gen;

  public OrganizationBuilder newOrg(String orgID) {
    return new OrganizationBuilder(orgID);
  }

  public OrganizationBuilder updateOrg(OrgMetadata org) {
    return new OrganizationBuilder(org);
  }

  public class Organization {
    private final OrgMetadata metadata;
    private final Map<String, Metric> schemas;

    private Organization(OrgMetadata metadata, Map<String, Metric> schemas) {
      this.metadata = metadata;
      this.schemas = schemas;
    }

    public OrgMetadata getMetadata() {
      return metadata;
    }

    /**
     * @return map of the canonical name to the metric
     */
    public Map<String, Metric> getSchemas() {
      return schemas;
    }
  }

  /**
   * Build or update the metric schemas for an organization
   */
  public class OrganizationBuilder {
    private final OrgMetadata.Builder org;
    private final Metadata.Builder meta;
    private Map<String, Metric> schemas = new HashMap<>();

    private OrganizationBuilder(OrgMetadata org) {
      this.org = OrgMetadata.newBuilder(org);
      this.meta = Metadata.newBuilder(org.getMetadata());
    }

    public OrganizationBuilder(String id) {
      this.meta = Metadata.newBuilder().setCanonicalName(id);
      this.org = OrgMetadata.newBuilder();
    }

    private void addMetric(Metric metric, OrgMetricMetadata metadata) {
      String id = metric.getMetadata().getCanonicalName();
      Map<String, OrgMetricMetadata> names = getMetrics();
      checkArgument(!names.containsKey(id), "Already have id: %s" + id);
      names.put(id, metadata);
      schemas.put(id, metric);
    }

    private void updateMetadata(Metric metric, OrgMetricMetadata.Builder metricMetadata) {
      String id = metric.getMetadata().getCanonicalName();
      Map<String, OrgMetricMetadata> metrics = getMetrics();
      metrics.put(id, metricMetadata.build());
      schemas.put(id, metric);
    }

    private Map<String, OrgMetricMetadata> getMetrics() {
      if (org.getMetrics() == null) {
        org.setMetrics(new HashMap<>());
      }
      return org.getMetrics();
    }

    public MetricBuilder newMetric() {
      return new MetricBuilder(this);
    }

    public MetricBuilder updateSchema(Metric previous) {
      Map<String, OrgMetricMetadata> metrics = getMetrics();
      String schemaName = previous.getMetadata().getCanonicalName();
      OrgMetricMetadata metadata = checkNotNull(metrics.get(schemaName),
        "Don't already have a field with id %s, cannot add an existing metric type for a name we "
        + "don't know about", schemaName);
      schemas.put(schemaName, previous);
      return new MetricBuilder(this, previous, metadata);
    }

    /**
     * Actually doesn't delete the metric. Instead, we just remove any possible name mappings for
     * the metric, but otherwise keeps the mapping. Ensures that we don't have duplicate cnames and
     * makes the update logic simpler
     *
     * @param metric to "delete"
     * @return
     */
    public OrganizationBuilder deleteMetric(Metric metric) {
      String name = metric.getMetadata().getCanonicalName();
      // remove the canonical name mapping -> so no aliases exist to support this metric.
      org.getMetrics().put(name, OrgMetricMetadata.newBuilder().build());
      return this;
    }

    public Organization build() {
      return new Organization(org.build(), schemas);
    }
  }

  /**
   * Builder for a metric type that has aliased fields and a schema based on canonical field names.
   * <p>
   * A "metric" is a metric type that includes a key things
   * <ol>
   * <li>the canonical name for this metric type</li>
   * <li>the aliases by which this</li>
   * </ol>
   * </p>
   */
  public class MetricBuilder {
    private final OrganizationBuilder parent;

    private final String orgId;
    private String canonicalName;

    private final Metric.Builder metric;
    private final OrgMetricMetadata.Builder metadata;
    private final boolean update;

    private List<FieldBuilder> newFields = new ArrayList<>();
    private List<FieldBuilder> updatedFields = new ArrayList<>();


    public MetricBuilder(OrganizationBuilder parent) {
      this(parent, Metric.newBuilder(), OrgMetricMetadata.newBuilder(), false);
    }

    public MetricBuilder(OrganizationBuilder parent, Metric previous, OrgMetricMetadata
      previousMetadata) {
      this(parent, Metric.newBuilder(previous), OrgMetricMetadata.newBuilder(previousMetadata),
        false);
    }

    private MetricBuilder(OrganizationBuilder parent, Metric.Builder metric,
      OrgMetricMetadata.Builder metadata, boolean update) {
      this.parent = parent;
      this.orgId = parent.org.getMetadata().getCanonicalName();
      this.metric = metric;
      this.metadata = metadata;
      this.update = update;
      this.canonicalName =
        metric.getMetadata() == null || metric.getMetadata().getCanonicalName() == null ?
        gen.generateSchemaName() :
        metric.getMetadata().getCanonicalName();
    }

    public OrganizationBuilder build() throws IOException {
      checkArgument(update || displayName != null,
        "Must have at least one name for the metric types when not updating a schema");
      // build the schema based on the newFields given
      AvroSchemaInstanceBuilder instance =
        new AvroSchemaInstanceBuilder(metric.getMetricSchema(), orgId, canonicalName);

      // do any updates we need for the updated records
      updatedFields.forEach(field -> {
        switch (field.delete) {
          case HARD:
            instance.deleteField(field.canonicalName);
            metric.getMetadata().getCanonicalNamesToAliases().remove(field.canonicalName);
            break;
          case SOFT:
            hide(field);
            break;
          case NONE:
          default:
            return;
        }
      });

      // add fields from the base record so we have the name mapping
      final Map<String, List<String>> cnameToAliases =
        getBuilderMetadata().getCanonicalNamesToAliases();
      instance.getBaseFieldNames().stream().forEach(name -> {
          List<String> aliases = cnameToAliases.get(name);
          if (aliases == null) {
            aliases = new ArrayList<>();
            cnameToAliases.put(name, aliases);
          }
        }
      );

      newFields.stream().forEach(field -> {
        instance.newField()
                .name(String.valueOf(field.canonicalName))
                .type(field.type)
                .done();
      });

      Schema recordSchema = instance.build();

      // generate the final metric
      metric.setMetricSchema(recordSchema.toString());
      metric.getMetadata().setCanonicalName(canonicalName);
      if (update) {
        metric.getMetadata().setVersion(inc(metric.getMetadata()));
        parent.updateMetadata(metric.build(), displayName, newAliases);
      } else {
        parent.addMetric(metric.build(), displayName, newAliases);
      }
      return parent;
    }

    private String inc(Metadata meta) {
      String version = meta.getVersion();
      int i = Integer.parseInt(version);
      return Integer.toString(++i);
    }

    private void hide(FieldBuilder field) {
      Map<String, Long> hiddenTime = this.metric.getHiddenTime();
      if (hiddenTime == null) {
        hiddenTime = new HashMap<>();
        this.metric.setHiddenTime(hiddenTime);
      }

      hiddenTime.put(field.canonicalName, System.currentTimeMillis());
    }

    public MetricBuilder withName(String name) {
      if (!update && displayName == null) {
        withDisplayName(name);
      } else {
        this.newAliases.add(name);
      }
      return this;
    }

    public MetricBuilder withDisplayName(String name) {
      displayName = name;
      return this;
    }

    private void updateField(FieldBuilder field) {
      Preconditions.checkNotNull(field.canonicalName,
        "Field wants to be updates, but doesn't have a canonical name!");
      String id = field.canonicalName;
      Map<String, List<String>> names = getBuilderMetadata().getCanonicalNamesToAliases();
      names.put(id, getAliasNames(checkHasAliases(names, id), field.displayName, field.aliases));
      this.updatedFields.add(field);
    }

    private void addField(FieldBuilder field) {
      Metadata metadata = getBuilderMetadata();
      field.aliases.add(field.displayName);
      checkAliasesDoNotAlreadyExist(metadata.getCanonicalNamesToAliases(), field.aliases);
      // find an open canonical name
      String fieldName;
      while (true) {
        // find an empty name
        fieldName = gen.generateSchemaName();
        Map<String, List<String>> fields = metadata.getCanonicalNamesToAliases();
        if (fields.get(fieldName) == null) {
          fields.put(fieldName,
            getAliasNames(Collections.EMPTY_LIST, field.displayName, field.aliases));
          break;
        }
      }
      field.canonicalName = fieldName;
      this.newFields.add(field);
    }

    private Metadata getBuilderMetadata() {
      Metadata metadata = this.metric.getMetadata();
      if (metadata == null) {
        metadata = emptyMetadata();
        this.metric.setMetadata(metadata);
      }

      return metadata;
    }

    public FieldBuilder withBoolean(String name) {
      return new FieldBuilder(this, name, "boolean");
    }

    public FieldBuilder withInt(String name) {
      return new FieldBuilder(this, name, "int");
    }

    public FieldBuilder withLong(String name) {
      return new FieldBuilder(this, name, "long");
    }

    public FieldBuilder withFloat(String name) {
      return new FieldBuilder(this, name, "float");
    }

    public FieldBuilder withDouble(String name) {
      return new FieldBuilder(this, name, "double");
    }

    public FieldBuilder withBytes(String name) {
      return new FieldBuilder(this, name, "bytes");
    }

    public FieldBuilder withString(String name) {
      return new FieldBuilder(this, name, "string");
    }

    public FieldBuilder updateField(String fieldCanonicalName) {
      Map<String, List<String>> fields =
        this.metric.getMetadata().getCanonicalNamesToAliases();
      List<String> aliases = fields.get(fieldCanonicalName);
      checkArgument(aliases != null, "No field with canonical name: %s",
        fieldCanonicalName);

      // get the properties of the known field from the schema
      Schema record = getRecordSchema();
      Schema.Field field = record.getField(fieldCanonicalName);
      String type = field.schema().getType().getName();
      return new FieldBuilder(this, fieldCanonicalName, aliases, type);
    }

    private Schema getRecordSchema() {
      String sSchema = metric.getMetricSchema();
      Schema.Parser parser = new Schema.Parser();
      parser.parse(sSchema);
      return parser.getTypes().get(
        SchemaNameUtils
          .getCustomerSchemaFullName(orgId, metric.getMetadata().getCanonicalName()));
    }
  }

  private enum Delete {
    NONE, HARD, SOFT;
  }

  private Metadata emptyMetadata() {
    return Metadata.newBuilder()
                   .setCanonicalName("to-be-replaced")
                   .setCanonicalNamesToAliases(new HashMap<>())
                   .build();
  }


  /**
   * Builder for fields that should be tracked in each metric type.
   */
  public class FieldBuilder {
    private final String type;
    private String canonicalName;
    private Set<String> aliases;
    private MetricBuilder metadata;
    private String displayName;
    public Delete delete = Delete.NONE;

    private FieldBuilder(MetricBuilder fields, String name, String type) {
      this.metadata = fields;
      this.type = type;
      aliases = new HashSet<>();
      this.displayName = name;
    }

    private FieldBuilder(MetricBuilder metricBuilder, String canonicalName, List<String> aliases,
      String type) {
      this.type = type;
      this.displayName = aliases.get(0);
      this.aliases = newHashSet(aliases);
      this.metadata = metricBuilder;
      this.canonicalName = canonicalName;
    }

    public FieldBuilder withAlias(String name) {
      this.aliases.add(name);
      return this;
    }

    public MetricBuilder asField() {
      if (this.canonicalName == null) {
        metadata.addField(this);
      } else {
        this.metadata.updateField(this);
      }
      return this.metadata;
    }

    public MetricBuilder softDelete() {
      this.delete = Delete.SOFT;
      return this.asField();
    }

    public MetricBuilder hardDelete() {
      this.delete = Delete.HARD;
      return this.asField();
    }

    public FieldBuilder withName(String displayName) {
      if (this.displayName != null) {
        this.aliases.add(this.displayName);
      }
      this.displayName = displayName;
      return this;
    }
  }

  private static List<String> getAliasNames(List<String> existingAliases, String displayName,
    Set<String> aliases) {
    if (displayName == null) {
      displayName = existingAliases.get(0);
    }
    aliases.addAll(existingAliases);
    aliases.remove(displayName);
    List<String> ret = new ArrayList<>(aliases.size() + 1);
    ret.add(displayName);
    ret.addAll(aliases);
    return ret;
  }

  private static OrgMetricMetadata checkHasAliases(Map<String, OrgMetricMetadata> names,
    String id) {
    OrgMetricMetadata metadata = names.get(id);
    checkArgument(metadata != null,
      "Must already have id (%s) stored for updating metric", id);
    return metadata;
  }

  private static void checkAliasesDoNotAlreadyExist(
    Map<String, OrgMetricMetadata> idToAliases, Collection<String> aliases) {
    Stream<Map.Entry<String, OrgMetricMetadata>> stream = idToAliases.entrySet().stream();
    for (String alias : aliases) {
      stream = stream.filter(SchemaUtils.metricHasAlias(alias));
    }
    Optional<Map.Entry<String, OrgMetricMetadata>> optional = stream.findAny();
    checkArgument(!optional.isPresent(), "[%s] already exists!",
      optional.get().getValue().getAliasValues());
  }
}
