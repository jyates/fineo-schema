package io.fineo.schema.avro;

import com.google.common.collect.Lists;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaTestUtils;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test that we can dynamically create, add and delete fields from the schema
 */
public class TestAvroSchemaInstanceBuilder {

  private static final Log LOG = LogFactory.getLog(TestAvroSchemaInstanceBuilder.class);
  public static final String SIMPLE_FIELD_CNAME = "canonical_name";
  public static final String SIMPLE_FIELD_TYPE = "int";
  private final String customerID = "a123x2g";
  private final String metricName = "a1";

  @Test
  public void testNewType() throws Exception {
    AvroSchemaInstanceBuilder builder = buildBasicSchema();
    Schema built = builder.build();

    String ns = SchemaNameUtils.getCustomerNamespace(customerID);
    String fullName = SchemaNameUtils.getCustomerSchemaFullName(customerID, metricName);
    assertEquals(ns, built.getNamespace());
    assertEquals(fullName, built.getFullName());
    assertEquals(metricName, built.getName());

    assertEquals(2, built.getFields().size());

    Schema.Field base = built.getField(AvroSchemaProperties.BASE_FIELDS_KEY);
    assertNotNull("Missing base field!", base);

    Schema.Field added = built.getField(SIMPLE_FIELD_CNAME);
    assertNotNull("Didn't add the custom field", added);
    SchemaTestUtils.verifyFieldType(SIMPLE_FIELD_TYPE, added);
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
    for(Schema.Field field : built2.getFields()){
      // skip base fields
      if(field.name().equals(AvroSchemaProperties.BASE_FIELDS_KEY)){
        continue;
      }
      SchemaTestUtils.verifyFieldType(expectedFields.get(field.name()), field);
    }
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
    map.put(AvroSchemaProperties.BASE_FIELDS_KEY, "record");
    return map;
  }

  private AvroSchemaInstanceBuilder buildBasicSchema() throws IOException {
    // then build the schema manually
    AvroSchemaInstanceBuilder builder = new AvroSchemaInstanceBuilder()
      .withNamespace(customerID)
      .withName(metricName);

    // check to make sure we have the expected base field
    assertEquals(Lists.newArrayList(AvroSchemaProperties.BASE_FIELDS_KEY),
      Lists.newArrayList(builder.getBaseFieldNames()));

    // add fields
    builder.newField().name(SIMPLE_FIELD_CNAME).type(SIMPLE_FIELD_TYPE).done();
    return builder;
  }


  private AvroSchemaInstanceBuilder getInstance(Schema schema) throws IOException {
    return new AvroSchemaInstanceBuilder(schema.toString(),
      SchemaNameUtils.getNameParts(schema.getNamespace()).getValue(), schema.getName());
  }
}
