package io.fineo.schema.avro;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public class TestAvroSchemaBuilder {

  @Test
  public void testNewType() throws Exception {
    // read in the avro schema we expect it to be
    final Schema.Parser parser = new Schema.Parser();
    parser.parse(getFile("new-type-expected-schema.avsc"));
    String customerID = "123x2g";
    String ns = SchemaUtils.getCustomerNamespace(customerID);
    String name = "a1";
    final Schema fromFile = parser.getTypes().get(ns + "." + name);

    // then build the schema manually
    AvroSchemaInstanceBuilder builder =
      new AvroSchemaInstanceBuilder(new SchemaNameGenerator()).withNamespace(customerID);

    // check to make sure we have the expected base fields
    assertEquals(Lists.newArrayList("hidden", "hidden_time", "unknown_fields"),
      Lists.newArrayList(builder.getBaseFieldNames()));

    // add fields
    builder.newField().name("somename").type("int").done();

    SchemaNameGenerator gen = Mockito.mock(SchemaNameGenerator.class);
    Mockito.when(gen.generateSchemaName()).thenReturn(name);
    builder.setSchemaNameGeneratorForTesting(gen);

    Schema built = builder.build();
    assertEquals("Schemas don't match! Expected: \n" + fromFile.toString(true) + "\n --- \n" + built
      .toString(true), fromFile, built);
  }

  @Test
  public void testChangeFieldName() throws Exception {
    fail("Give the builder a previous schema instance");
    fail("Set the new field name");
  }

  @Test
  public void testAddFieldAliasNoFieldChange() throws Exception {
    fail("Give the builder a previous schema instance");
    fail("Add a field alias, but don't change the field name");
  }

  @Test
  public void testAddField() throws Exception {
    fail("Add a new field to a previous schema");
  }

  @Test
  public void testRemoveField() throws Exception {
    fail("Remove a field from the schema as hidden");
  }

  private File getFile(String testPath) {
    ClassLoader classLoader = getClass().getClassLoader();
    return new File(classLoader.getResource(testPath).getFile());
  }
}
