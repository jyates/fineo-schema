package io.fineo.schema.avro;

import java.util.UUID;

/**
 *
 */
public class SchemaNameGenerator {

  public String generateSchemaName() {
    return AvroSchemaInstanceBuilder.PARENT_NAME_PREFIX +
           Math.abs(UUID.randomUUID().toString().hashCode());
  }
}
