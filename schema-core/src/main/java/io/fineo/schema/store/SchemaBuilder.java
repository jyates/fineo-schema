package io.fineo.schema.store;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
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
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

/**
 * Builder to generate a storable (in a {@link SchemaStore}) schema and organization/metric type
 * heirachy. Can also be used to update existing schemas bound to a metric or organization.
 *
 * @deprecated Use {@link StoreManager} instead
 */
@Deprecated
public class SchemaBuilder {

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

  public OrganizationBuilder updateOrg(Metadata org) {
    return new OrganizationBuilder(org);
  }

  public class Organization {
    private final Metadata metadata;
    private final Map<String, Metric> schemas;

    private Organization(Metadata metadata, Map<String, Metric> schemas) {
      this.metadata = metadata;
      this.schemas = schemas;
    }

    public Metadata getMetadata() {
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
    private final Metadata.Builder org;
    private Map<String, Metric> schemas = new HashMap<>();

    private OrganizationBuilder(Metadata org) {
      this.org = Metadata.newBuilder(org);
    }

    public OrganizationBuilder(String id) {
      this.org = Metadata.newBuilder().setCanonicalName(id);
    }

    private void addMetadata(Metric metric, String displayName, Set<String> aliases) {
      String id = metric.getMetadata().getCanonicalName();
      Map<String, List<String>> names = getBuilderMetricTypes();
      checkArgument(!names.containsKey(id), "Already have id: %s" + id);
      aliases.add(displayName);
      checkAliasesDoNotAlreadyExist(names, aliases);
      names.put(id, getAliasNames(Collections.emptyList(), displayName, aliases));
      schemas.put(id, metric);
    }

    private void updateMetadata(Metric metric, String displayName, Set<String> newAliases) {
      String id = metric.getMetadata().getCanonicalName();
      Map<String, List<String>> names = getBuilderMetricTypes();
      names.put(id, getAliasNames(checkHasAliases(names, id), displayName, newAliases));
      schemas.put(id, metric);
    }

    private Map<String, List<String>> getBuilderMetricTypes() {
      if (org.getCanonicalNamesToAliases() == null) {
        org.setCanonicalNamesToAliases(new HashMap<>());
      }
      return org.getCanonicalNamesToAliases();
    }

    public MetricBuilder newSchema() {
      return new MetricBuilder(this);
    }

    public MetricBuilder updateSchema(Metric previous) {
      Map<String, List<String>> names = getBuilderMetricTypes();
      String schemaName = previous.getMetadata().getCanonicalName();
      checkArgument(names.containsKey(schemaName),
        "Don't already have a field with id %s, cannot add an existing metric type for a name we "
        + "don't know about", schemaName);
      schemas.put(schemaName, previous);
      return new MetricBuilder(this, previous);
    }

    /**
     * Actually doesn't delete the metric. Instead, we just remove any possible name mappings for
     * the metric, but otherwise keeps the mapping. Ensures that we don't have duplicate cnames and
     * makes the update logic simpler
     * @param metric to "delete"
     * @return
     */
    public OrganizationBuilder deleteMetric(Metric metric) {
      String name = metric.getMetadata().getCanonicalName();
      // remove the canonical name mapping -> so no aliases exist to support this metric.
      org.getCanonicalNamesToAliases().put(name, newArrayList());
      return this;
    }

    public Organization build() {
      return new Organization(org.build(), schemas);
    }
  }

  /**
   * Builder for a metric type that has aliased fields and a schema based on canonical field names.
   * <p>
   * A "metadata" is a metric type that includes a key things
   * <ol>
   * <li>the canonical name for this metric type</li>
   * <li>the aliases by which this</li>
   * </ol>
   * </p>
   */
  public class MetricBuilder {
    private final String orgId;
    private final OrganizationBuilder parent;
    private final Metric.Builder metadata;
    private final boolean update;
    private Set<String> newAliases = new HashSet<>();
    private List<FieldBuilder> newFields = new ArrayList<>();
    private List<FieldBuilder> updatedFields = new ArrayList<>();
    private String recordName;
    private String displayName;

    private MetricBuilder(OrganizationBuilder parent, Metric.Builder builder, boolean update) {
      this.parent = parent;
      this.orgId = parent.org.getCanonicalName();
      this.metadata = builder;
      this.update = update;
      this.recordName =
        builder.getMetadata() == null || builder.getMetadata().getCanonicalName() == null ?
        gen.generateSchemaName() :
        builder.getMetadata().getCanonicalName();
    }

    String getCanonicalName() {
      return recordName;
    }

    public MetricBuilder(OrganizationBuilder parent) {
      this(parent, Metric.newBuilder(), false);
    }

    public MetricBuilder(OrganizationBuilder parent, Metric previous) {
      this(parent, Metric.newBuilder(previous), true);
    }

    public OrganizationBuilder build() throws IOException {
      checkArgument(update || displayName != null,
        "Must have at least one name for the metadata types when not updating a schema");
      // build the schema based on the newFields given
      AvroSchemaInstanceBuilder instance =
        new AvroSchemaInstanceBuilder(metadata.getMetricSchema(), orgId, recordName);

      // do any updates we need for the updated records
      updatedFields.forEach(field -> {
        switch (field.delete) {
          case HARD:
            instance.deleteField(field.canonicalName);
            break;
          case SOFT:
            MetricBuilder.this.hide(field);
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

      // generate the final metadata
      metadata.setMetricSchema(recordSchema.toString());
      metadata.getMetadata().setCanonicalName(recordName);
      if (update) {
        metadata.getMetadata().setVersion(inc(metadata.getMetadata()));
        parent.updateMetadata(metadata.build(), displayName, newAliases);
      } else {
        parent.addMetadata(metadata.build(), displayName, newAliases);
      }
      return parent;
    }

    private String inc(Metadata meta) {
      String version = meta.getVersion();
      int i = Integer.parseInt(version);
      return Integer.toString(++i);
    }

    private void hide(FieldBuilder field) {
      Map<String, Long> hiddenTime = this.metadata.getHiddenTime();
      if (hiddenTime == null) {
        hiddenTime = new HashMap<>();
        this.metadata.setHiddenTime(hiddenTime);
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
      Metadata metadata = this.metadata.getMetadata();
      if (metadata == null) {
        metadata = emptyMetadata();
        this.metadata.setMetadata(metadata);
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
        this.metadata.getMetadata().getCanonicalNamesToAliases();
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
      String sSchema = metadata.getMetricSchema();
      Schema.Parser parser = new Schema.Parser();
      parser.parse(sSchema);
      return parser.getTypes().get(
        SchemaNameUtils
          .getCustomerSchemaFullName(orgId, metadata.getMetadata().getCanonicalName()));
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

  private static List<String> checkHasAliases(Map<String, List<String>> names, String id) {
    List<String> currentAliases = names.get(id);
    checkArgument(currentAliases != null,
      "Must already have id (%s) stored for updating metric", id);
    return currentAliases;
  }

  private static void checkAliasesDoNotAlreadyExist(
    Map<String, ? extends Collection<String>> idToAliases,
    Collection<String> aliases) {
    String existingId = null, existingAlias = null;
    for (Map.Entry<String, ? extends Collection<String>> entry : idToAliases.entrySet()) {
      for (String alias : aliases) {
        if (entry.getValue().contains(alias)) {
          existingId = entry.getKey();
          existingAlias = alias;
          break;
        }
      }
    }

    checkArgument(existingId == null, "[%s] already exists!", existingAlias);
  }
}
