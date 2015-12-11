package io.fineo.schema.avro;


import io.fineo.internal.customer.BaseRecord;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Build a Fineo-style schema.
 * <p>
 * Under the hood, its actually an instance of an Avro {@link Schema} and a record with that
 * schema. That is necessary because Avro aliases do not support a full string, but only a subset
 * (see {@link Schema} internals for specifics), so a 'Fineo' schema actually keeps track of all
 * the different aliases for which fields can also be known. Thus, when you rehydrate this
 * 'schema' you actually get back a full fledged record.
 * </p>
 */
public class AvroSchemaInstanceBuilder {

  private final Schema baseSchema;
  private String namespace;
  private String schemaName;
  private List<AvroFieldBuilder> fields = new ArrayList<>();
  private List<String> deletedFields = new ArrayList<>();

  public AvroSchemaInstanceBuilder() throws IOException {
    this(null, null, null);
  }

  public AvroSchemaInstanceBuilder(String metricSchema, String orgId, String name) throws IOException {
    this.namespace = orgId == null? null: SchemaNameUtils.getCustomerNamespace(orgId);
    this.schemaName = name;
    // get the base-schema regardless of how we name the final record
    if (metricSchema == null || metricSchema.length() == 0) {
      this.baseSchema = BaseRecord.getClassSchema();
    } else {
      Schema.Parser parser = new Schema.Parser();
      parser.parse(metricSchema);
      this.baseSchema = parser.getTypes().get(SchemaNameUtils.getCustomerSchemaFullName(orgId, name));
    }
  }

  public Collection<String> getBaseFieldNames() {
    return this.baseSchema.getFields().stream().map(field -> field.name())
                          .collect(Collectors.toList());
  }

  public AvroSchemaInstanceBuilder withNamespace(String customerId) {
    this.namespace = SchemaNameUtils.getCustomerNamespace(customerId);
    return this;
  }

  public AvroSchemaInstanceBuilder withName(String recordName) {
    this.schemaName = recordName;
    return this;
  }

  public AvroFieldBuilder newField() {
    AvroFieldBuilder field = new AvroFieldBuilder(this);
    return field;
  }

  /**
   * Does a 'hard delete' of this field. Field will never be found again when reading data using
   * this schema. Instead, you probably want to 'hide' the field using the schema metadata.
   *
   * @param fieldCName name of the field to delete
   * @return <tt>this</tt>
   */
  public AvroSchemaInstanceBuilder deleteField(String fieldCName) {
    this.deletedFields.add(fieldCName);
    return this;
  }

  public Schema build() {
    SchemaBuilder.RecordBuilder<Schema> builder =
      SchemaBuilder.record(schemaName).namespace(namespace);
    final SchemaBuilder.FieldAssembler<Schema> assembler = builder.fields();
    // add the fields in the baseSchema record
    this.baseSchema.getFields().stream()
                   .forEach(field -> {
                     // we actually want to delete that field, so skip it
                     if (deletedFields.contains(field.name())) {
                       return;
                     }
                     assembler.name(field.name()).type(field.schema()).noDefault();
                   });


    // add each field from the field builder
    for (AvroFieldBuilder field : fields) {
      field.build(assembler);
    }
    return assembler.endRecord();
  }

  public class AvroFieldBuilder {
    protected final AvroSchemaInstanceBuilder parent;
    protected String type;
    protected String name;

    private AvroFieldBuilder(AvroSchemaInstanceBuilder parent) {
      this.parent = parent;
    }

    public AvroFieldBuilder type(String type) {
      this.type = type;
      return this;
    }

    public AvroFieldBuilder name(String fieldName) {
      this.name = fieldName;
      return this;
    }

    public AvroSchemaInstanceBuilder done() {
      parent.fields.add(this);
      return parent;
    }

    protected SchemaBuilder.FieldAssembler<Schema> build(
      SchemaBuilder.FieldAssembler<Schema> assembler) {
      return assembler.name(name).type(type).noDefault();
    }
  }
}