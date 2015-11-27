package io.fineo.schema.avro;

import java.util.UUID;

/**
 * Generate a unique name for a schema elemenet
 */
public class SchemaNameGenerator {

  private static final String NAME_PREFIX = "n";

  public String generateSchemaName() {
    return NAME_PREFIX +
           Math.abs(UUID.randomUUID().toString().hashCode());
  }
}
