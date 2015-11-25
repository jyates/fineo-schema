package io.fineo.schema;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.metric.MetricField;
import io.fineo.internal.customer.metric.MetricMetadata;
import io.fineo.internal.customer.metric.OrganizationMetadata;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.avro.SchemaNameGenerator;
import javafx.util.Pair;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class SchemaBuilder {

  public static final String ORG_ID_KEY = "companykey";
  public static final String ORG_METRIC_TYPE_KEY = "metrictype";
  /** name of the field in the base-metric.avsc that stores a map of the unknown fields */
  public static final String UNKNWON_KEYS = "unknown_fields";

  public SchemaBuilder(SchemaNameGenerator gen) {
    this.gen = gen;
  }

  private final SchemaNameGenerator gen;

  public OrganizationBuilder newOrg(String orgID) {
    return new OrganizationBuilder(orgID);
  }

  public OrganizationBuilder updateOrg(OrganizationMetadata org) {
    return new OrganizationBuilder(org);
  }

  public class OrganizationBuilder {
    private final List<CharSequence> fields = new ArrayList<>();
    private final OrganizationMetadata.Builder org;

    public OrganizationBuilder(OrganizationMetadata org) {
      fields.addAll(org.getFieldTypes());
      this.org = OrganizationMetadata.newBuilder(org);
    }

    public OrganizationBuilder(String id) {
      this.org = OrganizationMetadata.newBuilder().setOrgId(id);
    }

    public OrganizationBuilder addMetadata(MetricMetadata metadata) {
      CharSequence id = metadata.getCannonicalname();
      if (fields.contains(id)) {
        throw new IllegalArgumentException("Already have a field with that id!");
      }
      fields.add(id);
      return this;
    }

    public OrganizationMetadata build() {
      return org.setFieldTypes(fields).build();
    }


    public MetadataBuilder newSchema() {
      return new MetadataBuilder((String) org.getOrgId());
    }
  }

  public class MetadataBuilder {
    private final String orgId;
    private MetricMetadata.Builder metadata = MetricMetadata.newBuilder();
    private List<CharSequence> names = new ArrayList<>();
    private List<Pair<MetricField, String>> fields = new ArrayList<>();

    public MetadataBuilder(String orgId) {
      this.orgId = orgId;
    }

    public MetricMetadata build() throws IOException {
      Preconditions
        .checkArgument(names.size() > 0, "Must have at least one name for the metadata types");

      List<MetricField> fieldSchema = fields.stream()
                                            .map(pair -> pair.getKey())
                                            .collect(Collectors.toList());
      // build the schema based on the fields given
      AvroSchemaInstanceBuilder instance = new AvroSchemaInstanceBuilder();
      // get the current fields so we can add them as metrics. Doesn't support nested fields yet
      fieldSchema.addAll(instance.getBaseFieldNames().stream()
                                 .map(field -> new MetricField(field,
                                   Collections.<CharSequence>emptyList()))
                                 .collect(Collectors.toList()));
      // start building the actual record
      instance.withNamespace(orgId);
      fields.stream().forEach(pair -> {
        instance.newField()
                .name(String.valueOf(pair.getKey().getCanonicalName()))
                .type(pair.getValue());
      });

      Schema recordSchema = instance.build();

      // generate the final metadata
      metadata.setAliases(names)
              .setCannonicalname(recordSchema.getFullName())
              .setSchema$(recordSchema.toString())
              .setFieldMap(fieldSchema);
      return metadata.build();
    }

    public MetadataBuilder withName(String name) {
      this.names.add(name);
      return this;
    }

    private void addField(MetricField field, String type) {
      this.fields.add(new Pair<>(field, type));
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

  public class FieldBuilder {
    private final MetricField field;
    private final String type;
    MetadataBuilder metadata;
    private List<CharSequence> aliases = new ArrayList<>();

    public FieldBuilder(MetadataBuilder fields, String name, String type) {
      aliases.add(name);
      this.field = new MetricField(gen.generateSchemaName(), aliases);
      this.metadata = fields;
      this.type = type;
    }

    public FieldBuilder withAlias(String name) {
      // aliases are other names, while the display name is known
      this.aliases.add(0, name);
      return this;
    }

    public MetadataBuilder asField() {
      field.setAliases(aliases);
      metadata.addField(field, type);
      return this.metadata;
    }
  }
}
