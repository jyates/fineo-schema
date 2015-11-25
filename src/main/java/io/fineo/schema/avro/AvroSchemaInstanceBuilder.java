package io.fineo.schema.avro;


import com.google.common.annotations.VisibleForTesting;
import javafx.util.Pair;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

  public static final String PARENT_NAME_PREFIX = "p";
  private static final String FIELD_NAME_PREFIX = "f";
  private String namespace;
  private String schemaName;
  private List<AvroFieldBuilder> fields = new ArrayList<>();
  private List<String> names = new ArrayList<>(1);
  private SchemaNameGenerator generator = new SchemaNameGenerator();

  public AvroSchemaInstanceBuilder withNamespace(String customerId) {
    this.namespace = SchemaUtils.getCustomerNamespace(customerId);
    return this;
  }

  /**
   * Set the names used in the schema. Can be used multiple times to specify the aliases, with
   * the last specified name as the 'display name' for the user.
   *
   * @param schemaName names that the schema can be known by
   * @return this
   */
  public AvroSchemaInstanceBuilder withSchemaName(String schemaName) {
    this.names.add(schemaName);
    return this;
  }

  public static AvroFieldBuilder newField() {
    return new AvroFieldBuilder();
  }

  public AvroSchemaInstanceBuilder withField(AvroFieldBuilder fieldBuilder) {
    this.fields.add(fieldBuilder);
    return this;
  }

  public SchemaDescription build() {
    String schemaName = generator.generateSchemaName();
    SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(schemaName).namespace(
      namespace);
    SchemaBuilder.FieldAssembler<Schema> assembler = builder.fields();
    assembler = addAliasArray(assembler);

    // add each field from the field builder
    int i = 0;
    List<Pair<String, String>> fieldAliases = new ArrayList<>();
    for (AvroFieldBuilder field : fields) {
      assembler = field.build(i++, assembler);
      fieldAliases.add(new Pair<>(field.name, field.id));
    }
    Schema schema = assembler.endRecord();
    return new SchemaDescription(schema, names, fieldAliases);
  }

  @VisibleForTesting
  public void setSchemaNameGeneratorForTesting(SchemaNameGenerator gen) {
    this.generator = gen;
  }

  public static class AvroFieldBuilder {

    private String type;
    private String name;
    // generated once we build the field
    private String id;

    private AvroFieldBuilder() {
    }

    public AvroFieldBuilder type(String type) {
      this.type = type;
      return this;
    }

    public AvroFieldBuilder name(String fieldName) {
      this.name = fieldName;
      return this;
    }

    public SchemaBuilder.FieldAssembler<Schema> build(int index,
      SchemaBuilder.FieldAssembler<Schema> assembler) {
      String name = FIELD_NAME_PREFIX + index;
      this.id = name;
      SchemaBuilder.FieldAssembler<SchemaBuilder.RecordDefault<Schema>> building = assembler
        .name(name).type().record(name)
        .fields()
        .name("value").type(this.type).noDefault()
        .name("hidden").type().nullable().booleanType().noDefault()
        .name("hidden_time").type().nullable().longType().noDefault();
      return addAliasArray(building).endRecord().noDefault();
    }
  }

  private static <R> SchemaBuilder.FieldAssembler<R> addAliasArray(
    SchemaBuilder.FieldAssembler<R> assembler) {
    return assembler.name("aliases").type().array().items().stringType().noDefault();
  }
}
