package io.fineo.schema.store;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.FieldNameMap;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.avro.SchemaNameUtils;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SchemaBuilder {

  public SchemaBuilder(SchemaNameGenerator gen) {
    this.gen = gen;
  }

  private final SchemaNameGenerator gen;

  public OrganizationBuilder newOrg(String orgID) {
    return new OrganizationBuilder(orgID);
  }

  public OrganizationBuilder updateOrg(Organization org) {
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

    public OrganizationBuilder(Organization org) {
      this.org = Metadata.newBuilder(org.getMetadata());
      org.getSchemas().entrySet().forEach(entry -> addExistingMetric(entry));
    }

    public OrganizationBuilder(String id) {
      this.org = Metadata.newBuilder().setCanonicalName(id);
    }

    private OrganizationBuilder addExistingMetric(Map.Entry<String, Metric> metric) {
      Map<String, List<String>> names = getBuilderMetricTypes();
      Preconditions.checkArgument(names.containsKey(metric.getKey()),
        "Don't already have a field with id %s, cannot add an existing metric type for a name we "
        + "don't know about", metric.getKey());
      schemas.put(metric.getKey(), metric.getValue());
      return this;
    }

    private void addMetadata(Metric metric, List<String> aliases) {
      String id = metric.getMetadata().getCanonicalName();
      Map<String, List<String>> names = getBuilderMetricTypes();
      Preconditions.checkArgument(!names.containsKey(id),
        "Non-updating metrics must not already have a name!");
      names.put(id, aliases);
      schemas.put(id, metric);
    }

    private void updateMetadata(Metric metric, List<String> aliases) {
      String id = metric.getMetadata().getCanonicalName();
      Map<String, List<String>> names = getBuilderMetricTypes();
      List<String> currentAliases = names.get(id);
      Preconditions.checkArgument(currentAliases != null,
        "Must already have id (%s) stored for updating metric", id);
      currentAliases.addAll(aliases);
      schemas.put(id, metric);
    }

    private Map<String, List<String>> getBuilderMetricTypes() {
      if (org.getMetricTypes() == null) {
        org.setMetricTypes(emptyFieldMap());
      }
      return org.getMetricTypes().getCanonicalNamesToAliases();
    }

    public Organization build() {
      return new Organization(org.build(), schemas);
    }


    public MetadataBuilder newSchema() {
      return new MetadataBuilder(this);
    }

    public MetadataBuilder updateSchema(String schemaName) {
      Preconditions.checkArgument(getBuilderMetricTypes().get(schemaName) != null,
        "Don't have a previous schema '%s'", schemaName);
      return new MetadataBuilder(this, this.schemas.get(schemaName));
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
  public class MetadataBuilder {
    private final String orgId;
    private final OrganizationBuilder parent;
    private final Metric.Builder metadata;
    private final boolean update;
    private List<String> names = new ArrayList<>();
    private List<FieldBuilder> newFields = new ArrayList<>();
    private List<FieldBuilder> updatedFields = new ArrayList<>();
    private String recordName;

    private MetadataBuilder(OrganizationBuilder parent, Metric.Builder builder, boolean update) {
      this.parent = parent;
      this.orgId = parent.org.getCanonicalName();
      this.metadata = builder;
      this.update = update;
      this.recordName =
        builder.getMetadata() == null || builder.getMetadata().getCanonicalName() == null ?
        gen.generateSchemaName() :
        builder.getMetadata().getCanonicalName();

    }

    public MetadataBuilder(OrganizationBuilder parent) {
      this(parent, Metric.newBuilder(), false);
    }

    public MetadataBuilder(OrganizationBuilder parent, Metric previous) {
      this(parent, Metric.newBuilder(previous), true);
    }

    public OrganizationBuilder build() throws IOException {
      Preconditions.checkArgument(update || names.size() > 0,
        "Must have at least one name for the metadata types when not updating a schema");
      // build the schema based on the newFields given
      AvroSchemaInstanceBuilder instance =
        new AvroSchemaInstanceBuilder(metadata.getMetricSchema(), orgId, recordName);

      // do any updates we need for the updated records
      updatedFields.forEach(field -> {
        if (field.delete == null) {
          return;
        }
        switch (field.delete) {
          case HARD:
            instance.deleteField(field.canonicalName);
            break;
          case SOFT:
            MetadataBuilder.this.hide(field);
            break;
          default:
            return;
        }
      });

      // add fields from the base record so we have the name mapping
      final Map<String, List<String>> cnameToAliases =
        getBuilderMetadata().getMetricTypes().getCanonicalNamesToAliases();
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
      metadata.getMetadata().setCanonicalName(recordName);
      metadata.setMetricSchema(recordSchema.toString());
      if (update) {
        parent.updateMetadata(metadata.build(), names);
      } else {
        parent.addMetadata(metadata.build(), names);
      }
      return parent;
    }

    private void hide(FieldBuilder field) {
      Map<String, Long> hiddenTime = this.metadata.getHiddenTime();
      if (hiddenTime == null) {
        hiddenTime = new HashMap<>();
        this.metadata.setHiddenTime(hiddenTime);
      }

      hiddenTime.put(field.canonicalName, System.currentTimeMillis());
    }

    public MetadataBuilder withName(String name) {
      this.names.add(name);
      return this;
    }

    private void addField(FieldBuilder field) {
      if (field.canonicalName != null) {
        this.updatedFields.add(field);
        return;
      }
      // find an open canonical name
      String cname;
      while (true) {
        cname = gen.generateSchemaName();
        // ensure we have some metadata
        Metadata metadata = getBuilderMetadata();
        Map<String, List<String>> fields = metadata.getMetricTypes().getCanonicalNamesToAliases();
        if (fields.get(cname) == null) {
          fields.put(cname, field.aliases);
          break;
        }
      }
      field.canonicalName = cname;
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
        this.metadata.getMetadata().getMetricTypes().getCanonicalNamesToAliases();
      List<String> aliases = fields.get(fieldCanonicalName);
      Preconditions
        .checkArgument(aliases != null, "No field with canonical name: %s",
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
        SchemaNameUtils.getCustomerSchemaFullName(orgId, metadata.getMetadata().getCanonicalName()));
    }
  }

  private enum Delete {
    HARD, SOFT;
  }

  private Metadata emptyMetadata() {
    return Metadata.newBuilder()
                   .setCanonicalName("to-be-replaced")
                   .setMetricTypes(emptyFieldMap())
                   .build();
  }

  private FieldNameMap emptyFieldMap() {
    return FieldNameMap.newBuilder().setCanonicalNamesToAliases(new HashMap<>()).build();
  }

  /**
   * Builder for fields that should be tracked in each metric type.
   */
  public class FieldBuilder {
    private final String type;
    private String canonicalName;
    private List<String> aliases;
    private MetadataBuilder metadata;
    private String displayName;
    public Delete delete;

    private FieldBuilder(MetadataBuilder fields, String name, String type) {
      this.metadata = fields;
      this.type = type;
      aliases = new ArrayList<>();
      this.displayName = name;
    }

    private FieldBuilder(MetadataBuilder metadataBuilder, String canonicalName,
      List<String> aliases,
      String type) {
      this.type = type;
      this.aliases = aliases;
      this.metadata = metadataBuilder;
      this.canonicalName = canonicalName;
    }

    public FieldBuilder withAlias(String name) {
      this.aliases.add(name);
      return this;
    }

    public MetadataBuilder asField() {
      this.aliases.add(displayName);
      metadata.addField(this);
      return this.metadata;
    }

    public MetadataBuilder softDelete() {
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
}
