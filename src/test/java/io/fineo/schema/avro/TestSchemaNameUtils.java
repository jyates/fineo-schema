package io.fineo.schema.avro;

import io.fineo.schema.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestSchemaNameUtils {

  @Test
  public void testToFromNames() throws Exception{
    String orgId = "someorg", metric = "metricname";
    String fullName = SchemaNameUtils.getCustomerSchemaFullName(orgId, metric);
    Pair<String, String> parts = SchemaNameUtils.getNameParts(fullName);
    assertEquals(metric, parts.getValue());
    assertEquals(orgId, SchemaNameUtils.getOrgId(parts.getKey()));
  }
}
