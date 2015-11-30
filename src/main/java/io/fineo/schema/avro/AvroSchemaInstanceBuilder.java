package io.fineo.schema.avro;


import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.File;
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

  private static final String BASE_SCHEMA_TYPE = "io.fineo.internal.customer.BaseRecord";
  private final String BASE_SCHEMA_FILE = "avro/base-metric.avsc";
  private final Schema base;
  private String namespace;
  private List<AvroFieldBuilder> fields = new ArrayList<>();
  private SchemaNameGenerator generator;

  public AvroSchemaInstanceBuilder(SchemaNameGenerator gen) throws IOException {
    Schema.Parser parser = new Schema.Parser();
    parser.parse(getFile(BASE_SCHEMA_FILE));
    this.base = parser.getTypes().get(BASE_SCHEMA_TYPE);
    this.generator = gen;
  }

  private File getFile(String testPath) {
    ClassLoader classLoader = getClass().getClassLoader();
    return new File(classLoader.getResource(testPath).getFile());
  }

  public AvroSchemaInstanceBuilder withNamespace(String customerId) {
    this.namespace = SchemaUtils.getCustomerNamespace(customerId);
    return this;
  }

  public AvroFieldBuilder newField() {
    AvroFieldBuilder field = new AvroFieldBuilder(this);
    return field;
  }

  public Schema build() {
    String schemaName = generator.generateSchemaName();
    SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(schemaName).namespace(
      namespace);
    final SchemaBuilder.FieldAssembler<Schema> assembler = builder.fields();
    // add the fields in the base record
    this.base.getFields().stream()
             .forEach(field -> {
               assembler.name(field.name()).type(field.schema()).noDefault();
             });


    // add each field from the field builder
    for (AvroFieldBuilder field : fields) {
      field.build(assembler);
    }
    return assembler.endRecord();
  }

  public Collection<String> getBaseFieldNames() {
    return this.base.getFields().stream().map(field -> field.name()).collect(Collectors.toList());
  }

  @VisibleForTesting
  public void setSchemaNameGeneratorForTesting(SchemaNameGenerator gen) {
    this.generator = gen;
  }

  public static class AvroFieldBuilder {
    private final AvroSchemaInstanceBuilder parent;
    private String type;
    private String name;

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

    public AvroSchemaInstanceBuilder done(){
      parent.fields.add(this);
      return parent;
    }

    private SchemaBuilder.FieldAssembler<Schema> build(
      SchemaBuilder.FieldAssembler<Schema> assembler) {
      return assembler.name(name).type(type).noDefault();
    }
  }
}
