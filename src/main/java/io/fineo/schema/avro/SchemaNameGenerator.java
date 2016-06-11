package io.fineo.schema.avro;

import java.util.UUID;

/**
 * Generate a unique name for a schema elemenet
 */
@FunctionalInterface
public interface SchemaNameGenerator {

  SchemaNameGenerator DEFAULT_INSTANCE =
    () -> "n" + Math.abs(UUID.randomUUID().toString().hashCode());

  String generateSchemaName();
}
