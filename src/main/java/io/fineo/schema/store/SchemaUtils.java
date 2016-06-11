package io.fineo.schema.store;

import io.fineo.internal.customer.Metadata;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.exception.SchemaNotFoundException;

import java.util.Map;

public class SchemaUtils {

  private SchemaUtils() {
  }

  public static String getCanonicalName(Metadata metadata, String aliasName) {
    Map<String, String> aliasToCname = AvroSchemaManager.getAliasRemap(metadata);
    return aliasToCname.get(aliasName);
  }

  public static <T> void checkFound(T canonical, String aliasName,
    String fieldDescription) throws SchemaNotFoundException {
    if (canonical == null) {
      throw new SchemaNotFoundException(String.format(
        "No %s found with name: '%s'", fieldDescription, aliasName));
    }
  }
}
