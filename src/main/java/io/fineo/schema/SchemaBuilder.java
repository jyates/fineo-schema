package io.fineo.schema;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.avro.SchemaNameGenerator;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SchemaBuilder {

  public static final String ORG_ID_KEY = "companykey";
  public static final String ORG_METRIC_TYPE_KEY = "metrictype";
  /**
   * name of the field in the base-metric.avsc that stores a map of the unknown fields
   */
  public static final String UNKNWON_KEYS = "unknown_fields";

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
    private final List<Metric> schemas;

    private Organization(Metadata metadata, List<Metric> schemas) {
      this.metadata = metadata;
      this.schemas = schemas;
    }

    public Metadata getMetadata() {
      return metadata;
    }

    public List<Metric> getSchemas() {
      return schemas;
    }
  }

  /**
   * Build or update the metric schemas for an organization
   */
  public class OrganizationBuilder {
    private final Metadata.Builder org;
    private List<Metric> schemas = new ArrayList<>();

    public OrganizationBuilder(Organization org) {
      org.getSchemas().forEach(metric -> addMetadataInternal(metric));
      this.org = Metadata.newBuilder(org.getMetadata());
    }

    public OrganizationBuilder(String id) {
      this.org = Metadata.newBuilder().setCanonicalName(id);
    }

    private OrganizationBuilder addMetadataInternal(Metric metric) {
      CharSequence id = metric.getMetadata().getCanonicalName();
      Map<String, List<String>> names =
        org.getMetricTypes().getCanonicalNamesToAliases();
      Preconditions.checkArgument(!names.containsKey(id), "Already have a field with id %s", id);
      schemas.add(metric);
      return this;
    }

    private OrganizationBuilder addMetadata(Metric metric, List<String> aliases) {
      String id = metric.getMetadata().getCanonicalName();
      Map<String, List<String>> names =
        org.getMetricTypes().getCanonicalNamesToAliases();
      if (names.containsKey(id)) {
        throw new IllegalArgumentException("Already have a field with that id!");
      }
      names.put(id, aliases);
      schemas.add(metric);
      return this;
    }

    public Organization build() {
      return new Organization(org.build(), schemas);
    }


    public MetadataBuilder newSchema() {
      return new MetadataBuilder((String) org.getCanonicalName(), this);
    }

    public MetadataBuilder updateSchema(String canonicalName) {
      return null;
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
    private Metric.Builder metadata = Metric.newBuilder();
    private List<String> names = new ArrayList<>();
    private List<FieldBuilder> newFields = new ArrayList<>();

    public MetadataBuilder(String orgId, OrganizationBuilder parent) {
      this.parent = parent;
      this.orgId = orgId;
    }

    public OrganizationBuilder build() throws IOException {
      Preconditions
        .checkArgument(names.size() > 0, "Must have at least one name for the metadata types");

      // build the schema based on the newFields given
      AvroSchemaInstanceBuilder instance = new AvroSchemaInstanceBuilder(gen);

      // add fields from the base record so we have the name mapping
      final Map<String, List<String>> cnameToAliases =
        metadata.getMetadata().getMetricTypes().getCanonicalNamesToAliases();
      instance.getBaseFieldNames().stream().forEach(name -> {
          List<String> aliases = cnameToAliases.get(name);
          if (aliases == null) {
            aliases = new ArrayList<>();
            cnameToAliases.put(name, aliases);
          }
        }
      );

      // start building the actual record
      instance.withNamespace(orgId);
      newFields.stream().forEach(field -> {
        instance.newField()
                .name(String.valueOf(field.canonicalName))
                .type(field.type)
                .done();
      });

      Schema recordSchema = instance.build();

      // generate the final metadata
      metadata.getMetadata().setCanonicalName(gen.generateSchemaName());
      metadata.setMetricSchema(recordSchema.toString());
      parent.addMetadata(metadata.build(), names);
      return parent;
    }

    public MetadataBuilder withName(String name) {
      this.names.add(name);
      return this;
    }

    private void addField(FieldBuilder field) {
      // find an open name
      String cname;
      while (true) {
        cname = gen.generateSchemaName();
        Map<String, List<String>> fields =
          metadata.getMetadata().getMetricTypes().getCanonicalNamesToAliases();
        if (fields.get(cname) == null) {
          fields.put(cname, field.aliases);
          break;
        }
      }
      field.canonicalName = cname;
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
  }

  /**
   * Builder for fields that should be tracked in each metric type.
   */
  public class FieldBuilder {
    private final String type;
    private String canonicalName;
    private List<String> aliases;
    MetadataBuilder metadata;

    public FieldBuilder(MetadataBuilder fields, String name, String type) {
      this.metadata = fields;
      this.type = type;
      aliases = new ArrayList<>();
      aliases.add(name);
    }

    public FieldBuilder withAlias(String name) {
      // aliases are other names, while the display name is known
      this.aliases.add(0, name);
      return this;
    }

    public MetadataBuilder asField() {
      metadata.addField(this);
      return this.metadata;
    }
  }
}
