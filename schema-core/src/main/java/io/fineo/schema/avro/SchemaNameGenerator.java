package io.fineo.schema.avro;

import io.fineo.schema.FineoStopWords;

import java.util.UUID;

/**
 * Generate a unique name for a schema elemenet
 */
@FunctionalInterface
public interface SchemaNameGenerator {

  SchemaNameGenerator DEFAULT_INSTANCE =
    () -> FineoStopWords.FIELD_PREFIX + "f" + Math.abs(UUID.randomUUID().toString().hashCode());

  String generateSchemaName();
}
