package io.fineo.schema.store;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.FieldGravestone;
import io.fineo.internal.customer.FieldGraveyard;
import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.Gravestone;
import io.fineo.internal.customer.Graveyard;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.MetricMetadata;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.avro.SchemaNameGenerator;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static io.fineo.schema.avro.SchemaNameUtils.getCustomerSchemaFullName;
import static java.lang.String.format;

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
      addMetricInternal(metric, metadata,
        (id, names) -> {
          checkArgument(!names.containsKey(id), "Already have metric id: '%s'", id);
          checkAliasesDoNotAlreadyExist(names, metadata);
        });
    }

    private void updateMetadata(Metric metric, OrgMetricMetadata metricMetadata) {
      addMetricInternal(metric, metricMetadata, (id, names) -> {
        Map<String, OrgMetricMetadata> copy = newHashMap(names);
        copy.remove(id);
        checkAliasesDoNotAlreadyExist(copy, metricMetadata);
      });
    }

    private void addMetricInternal(Metric metric, OrgMetricMetadata metadata, BiConsumer<String,
      Map<String, OrgMetricMetadata>> check) {
      String id = metric.getMetadata().getMeta().getCanonicalName();
      Map<String, OrgMetricMetadata> names = getMetrics();
      check.accept(id, names);
      names.put(id, metadata);
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
      String schemaName = previous.getMetadata().getMeta().getCanonicalName();
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
     * @return the update in progress for the org
     */
    public OrganizationBuilder deleteMetric(Metric metric) {
      String name = metric.getMetadata().getMeta().getCanonicalName();
      OrgMetricMetadata metricMetadata = org.getMetrics().remove(name);
      // at best, this is an approximation due to clock drift across machines... eh #startup
      long deleteTime = System.currentTimeMillis();
      Graveyard graveyard = org.getGraveyard();
      if (graveyard == null) {
        graveyard = Graveyard.newBuilder()
                             .setDeadMetrics(new HashMap<>()).build();
        org.setGraveyard(graveyard);
      }
      graveyard.getDeadMetrics().put(name, Gravestone.newBuilder()
                                                     .setDeadtime(deleteTime)
                                                     .setMetric(metricMetadata).build());
      return this;
    }

    public Organization build() {
      org.setMetadata(meta.build());
      return new Organization(org.build(), schemas);
    }

    private void checkAliasesDoNotAlreadyExist(Map<String, OrgMetricMetadata> metrics,
      OrgMetricMetadata toAdd) {
      checkArgument(!metrics.values().stream()
                            .flatMap(metric -> metric.getAliasValues().stream())
                            .anyMatch(alias -> toAdd.getAliasValues().contains(alias)),
        "Cannot add a metric with the same alias value as another metric,\n"
        + "e.g you cannot have two metrics: 't' and 'v', whose value for 'metrictype' field is "
        + "the same. There would be no way to disambiguate between the two.\nAttempted to add "
        + "alias(es): %s", toAdd.getAliasValues()
      );
    }

    public void addMetricKey(String key, String metricId) {
      getKeyMap().put(key, metricId);
    }

    public void removeMetricKey(String key, String metricId) {
      String id = getKeyMap().remove(key);
      // its not an alias for the metric - put it back and throw and error
      if (id != null && !id.equals(metricId)) {
        addMetricKey(key, id);
        throw new IllegalArgumentException(
          format("%s is not a key alias for specified metric ([id: %s]", key, metricId));
      }
    }

    private Map<String, String> getKeyMap() {
      Map<String, String> map = org.getMetricKeyMap();
      if (map == null) {
        map = new HashMap<>();
        org.setMetricKeyMap(map);
      }
      return map;
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

    private final OrgMetricMetadata.Builder metadata;
    private final Metric.Builder metric;
    private final MetricMetadata.Builder metricMetadata;
    private final boolean update;

    private List<FieldBuilder> newFields = new ArrayList<>();
    private List<FieldBuilder> updatedFields = new ArrayList<>();


    public MetricBuilder(OrganizationBuilder parent) {
      this(parent, Metric.newBuilder(), OrgMetricMetadata.newBuilder(), false);
    }

    public MetricBuilder(OrganizationBuilder parent, Metric previous, OrgMetricMetadata
      previousMetadata) {
      this(parent, Metric.newBuilder(previous), OrgMetricMetadata.newBuilder(previousMetadata),
        true);
    }

    private MetricBuilder(OrganizationBuilder parent, Metric.Builder metric,
      OrgMetricMetadata.Builder metadata, boolean update) {
      this.parent = parent;
      this.orgId = parent.meta.getCanonicalName();
      this.metric = metric;
      this.metadata = metadata;
      this.metricMetadata = metric.getMetadata() == null ?
                            MetricMetadata.newBuilder() :
                            MetricMetadata.newBuilder(metric.getMetadata());
      init(metricMetadata);
      this.update = update;
    }

    private void init(MetricMetadata.Builder metricMetadata) {
      if (!metricMetadata.hasFields()) {
        metricMetadata.setFields(new HashMap<>());
      }
      if (!metricMetadata.hasMeta()) {
        metricMetadata.setMeta(Metadata.newBuilder()
                                       .setCanonicalName(gen.generateSchemaName())
                                       .build());
      }
    }

    public OrganizationBuilder build() throws IOException {
      // either we are updating the metric or there is at lesat one name given for the metric
      checkArgument(update || (metadata.hasAliasValues() && metadata.getAliasValues().size() > 0),
        "Must have at least one name for the metric types when not updating a schema");
      // build the schema based on the newFields given
      AvroSchemaInstanceBuilder instance =
        new AvroSchemaInstanceBuilder(metric.getMetricSchema(), orgId,
          metricMetadata.getMeta().getCanonicalName());

      // do any updates we need for the updated records
      long deadTime = System.currentTimeMillis();
      updatedFields.forEach(field -> {
        switch (field.delete) {
          case HARD:
            instance.deleteField(field.canonicalName);
          case SOFT:
            FieldMetadata metadata = metricMetadata.getFields().remove(field.canonicalName);
            FieldGraveyard graveyard = metricMetadata.getGraveyard();
            if(graveyard == null){
              graveyard = FieldGraveyard.newBuilder().setDeadFields(new HashMap<>()).build();
             metricMetadata.setGraveyard(graveyard);
            }
            graveyard.getDeadFields().put(field.canonicalName,
              FieldGravestone.newBuilder().setDeadtime(deadTime).setField(metadata).build());
            break;
          case NONE:
          default:
            return;
        }
      });

      // add fields from the base record so we have the name mapping. We have already added
      // user-defined fields to the metadata in the field builder
      Map<String, FieldMetadata> fieldAliases = metricMetadata.getFields();
      instance.getBaseFieldNames().stream().forEach(name -> {
          FieldMetadata metadata = fieldAliases.get(name);
          if (metadata == null) {
            metadata = FieldMetadata.newBuilder()
                                    .setDisplayName(name)
                                    .setFieldAliases(new ArrayList<>())
                                    .build();
            fieldAliases.put(name, metadata);
          }
        }
      );

      // actually add the new fields to the instance schema
      newFields.stream().forEach(field -> {
        instance.newField()
                .name(String.valueOf(field.canonicalName))
                .type(field.type)
                .done();
      });

      Schema recordSchema = instance.build();

      // generate the final metric
      metric.setMetricSchema(recordSchema.toString());
      metric.setMetadata(metricMetadata.build());

      if (update) {
        metric.getMetadata().getMeta().setVersion(inc(metric.getMetadata()));
        parent.updateMetadata(metric.build(), this.metadata.build());
      } else {
        parent.addMetric(metric.build(), this.metadata.build());
      }
      return parent;
    }

    private String inc(MetricMetadata metadata) {
      return SchemaBuilder.inc(metadata.getMeta());
    }

    public MetricBuilder withName(String name) {
      // have to have a display name
      if (!update && metadata.getDisplayName() == null) {
        metadata.setDisplayName(name);
      }

      List<String> aliases = this.metadata.getAliasValues();
      if (aliases == null) {
        aliases = new ArrayList<>();
        this.metadata.setAliasValues(aliases);
      }
      if (!aliases.contains(name)) {
        aliases.add(name);
      }

      return this;
    }

    public MetricBuilder withDisplayName(String name) {
      withName(name);
      metadata.setDisplayName(name);
      return this;
    }

    private void updateField(FieldBuilder field) {
      String id = Preconditions.checkNotNull(field.canonicalName,
        "Field wants to be updates, but doesn't have a canonical name!");
      checkNotNull(this.metricMetadata.getFields().get(id),
        "No previous field found for canonical name: %s", field.canonicalName);
      // check that we didn't add any aliases that conflict with the existing aliases
      Map<String, FieldMetadata> map = newHashMap(this.metricMetadata.getFields());
      map.remove(id);
      checkAliasesDoNotAlreadyExist(map, field);
      this.metricMetadata.getFields().put(id, field.fieldMetadata.build());
      this.updatedFields.add(field);
    }

    private void addField(FieldBuilder field) {
      checkAliasesDoNotAlreadyExist(field);
      // find an open canonical name
      String fieldName;
      while (true) {
        // find an empty name
        fieldName = gen.generateSchemaName();
        Map<String, FieldMetadata> fields = metricMetadata.getFields();
        if (fields.get(fieldName) == null) {
          fields.put(fieldName, field.fieldMetadata.build());
          break;
        }
      }
      field.canonicalName = fieldName;
      this.newFields.add(field);
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
      Map<String, FieldMetadata> fields = metricMetadata.getFields();
      FieldMetadata fieldMetdata = checkNotNull(fields.get(fieldCanonicalName),
        "No field with canonical name: %s", fieldCanonicalName);

      // get the properties of the known field from the schema
      Schema record = getRecordSchema();
      Schema.Field field = record.getField(fieldCanonicalName);
      String type = field.schema().getType().getName();
      return new FieldBuilder(this, fieldCanonicalName, fieldMetdata, type);
    }

    private Schema getRecordSchema() {
      String sSchema = metric.getMetricSchema();
      Schema.Parser parser = new Schema.Parser();
      parser.parse(sSchema);
      return parser.getTypes().get(
        getCustomerSchemaFullName(orgId, metricMetadata.getMeta().getCanonicalName()));
    }

    public MetricMetadata.Builder getMetricMetadata() {
      return metricMetadata;
    }

    private void checkAliasesDoNotAlreadyExist(FieldBuilder field) {
      checkAliasesDoNotAlreadyExist(metricMetadata.getFields(), field);
    }

    private void checkAliasesDoNotAlreadyExist(Map<String, FieldMetadata> fields, FieldBuilder
      field) {
      checkArgument(!fields.values().stream()
                           .flatMap(inst -> inst.getFieldAliases().stream())
                           .anyMatch(name -> field.fieldMetadata.getFieldAliases().contains(name)),
        "Cannot have two fields in the same metric with the same alias name! Attempted to add "
        + "aliases: %s to existing aliases %s",
        field.fieldMetadata.getFieldAliases(),
        new AsyncToString(() -> fields.values().stream().flatMap(
          inst -> inst.getFieldAliases().stream()).collect(Collectors.toList())));
    }
  }

  private class AsyncToString {
    private final Supplier<Object> supplier;

    private AsyncToString(Supplier<Object> supplier) {
      this.supplier = supplier;
    }

    @Override
    public String toString() {
      return supplier.get().toString();
    }
  }

  private enum Delete {
    NONE, HARD, SOFT;
  }

  /**
   * Builder for fields that should be tracked in each metric type.
   */
  public class FieldBuilder {
    private final String type;
    private String canonicalName;
    public final FieldMetadata.Builder fieldMetadata;
    private MetricBuilder parent;
    public Delete delete = Delete.NONE;


    private FieldBuilder(MetricBuilder fields, String name, String type) {
      this.parent = fields;
      this.fieldMetadata = FieldMetadata.newBuilder()
                                        .setFieldAliases(newArrayList(name))
                                        .setDisplayName(name);
      this.type = type;
    }

    private FieldBuilder(MetricBuilder parent, String canonicalName, FieldMetadata field,
      String type) {
      this.type = type;
      this.parent = parent;
      this.canonicalName = canonicalName;
      this.fieldMetadata = FieldMetadata.newBuilder(field);
    }

    public MetricBuilder asField() {
      if (this.canonicalName == null) {
        parent.addField(this);
      } else {
        this.parent.updateField(this);
      }
      return this.parent;
    }

    public MetricBuilder hardDelete() {
      this.delete = Delete.HARD;
      return this.asField();
    }

    public FieldBuilder withName(String displayName) {
      this.fieldMetadata.setDisplayName(displayName);
      withAlias(displayName);
      return this;
    }

    public FieldBuilder withAlias(String name) {
      if (!this.fieldMetadata.getFieldAliases().contains(name)) {
        this.fieldMetadata.getFieldAliases().add(name);
      }
      return this;
    }
  }

  private static String inc(Metadata meta) {
    String version = meta.getVersion();
    int i = Integer.parseInt(version);
    return Integer.toString(++i);
  }
}
