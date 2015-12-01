package io.fineo.schema.avro;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test that we can dynamically create, add and delete fields from the schema
 */
public class TestAvroSchemaInstanceBuilder {

  private static final Log LOG = LogFactory.getLog(TestAvroSchemaInstanceBuilder.class);
  public static final String SIMPLE_FIELD_CNAME = "somename";
  public static final String SIMPLE_FIELD_TYPE = "int";
  private final String customerID = "a123x2g";
  private final String name = "a1";

  @Test
  public void testNewType() throws Exception {
    // read in the avro schema we expect it to be
    final Schema.Parser parser = new Schema.Parser();
    parser.parse(getFile("new-type-expected-schema.avsc"));
    String fullName = SchemaUtils.getCustomerSchemaFullName(customerID, name);
    final Schema fromFile = parser.getTypes().get(fullName);

    AvroSchemaInstanceBuilder builder = buildBasicSchema();
    Schema built = builder.build();
    assertEquals("Schemas don't match! Expected: \n" + fromFile.toString(true) + "\n --- \n" + built
      .toString(true), fromFile, built);
  }

  private File getFile(String testPath) {
    ClassLoader classLoader = getClass().getClassLoader();
    return new File(classLoader.getResource(testPath).getFile());
  }

  @Test
  public void testAddField() throws Exception {
    // start with a simple schema
    AvroSchemaInstanceBuilder builder = buildBasicSchema();
    Schema built = builder.build();

    builder = getInstance(built);
    // add a new field
    builder.newField().name("new_field").type("string").done();
    Schema built2 = builder.build();
    LOG.info("Built new schema: " + built2.toString(true));

    // verify that we add the field and have all the original fields
    assertEquals(3, built2.getFields().size());
    Map<String, String> expectedFields = getBaseTypeMap();
    expectedFields.put(SIMPLE_FIELD_CNAME, SIMPLE_FIELD_TYPE);
    expectedFields.put("new_field", "string");
    built2.getFields()
          .stream().forEach(
      field -> assertEquals(expectedFields.get(field.name()), field.schema().getType().getName()));
  }

  @Test
  public void testRemoveField() throws Exception {
    // start with a simple schema
    AvroSchemaInstanceBuilder builder = buildBasicSchema();
    Schema built = builder.build();

    builder = getInstance(built);
    // add a new field
    builder.deleteField(SIMPLE_FIELD_CNAME);
    Schema built2 = builder.build();

    // verify that we add the field and have all the original fields
    assertEquals(1, built2.getFields().size());
    Map<String, String> expectedFields = getBaseTypeMap();
    built2.getFields()
          .stream().forEach(
      field -> assertEquals(expectedFields.get(field.name()), field.schema().getType().getName()));
  }


  private Map<String, String> getBaseTypeMap() {
    Map<String, String> map = new HashMap<>();
    map.put("unknown_fields", "map");
    return map;
  }

  private AvroSchemaInstanceBuilder buildBasicSchema() throws IOException {
    // then build the schema manually
    AvroSchemaInstanceBuilder builder = new AvroSchemaInstanceBuilder()
      .withNamespace(customerID)
      .withName(name);

    // check to make sure we have the expected base fields
    assertEquals(Lists.newArrayList("unknown_fields"),
      Lists.newArrayList(builder.getBaseFieldNames()));

    // add fields
    builder.newField().name(SIMPLE_FIELD_CNAME).type(SIMPLE_FIELD_TYPE).done();
    return builder;
  }


  private AvroSchemaInstanceBuilder getInstance(Schema schema) throws IOException {
    return new AvroSchemaInstanceBuilder(schema.toString(),
      SchemaUtils.getNameParts(schema.getNamespace()).getValue(), schema.getName());
  }
}
