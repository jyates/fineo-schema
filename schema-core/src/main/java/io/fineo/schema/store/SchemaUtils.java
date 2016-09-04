package io.fineo.schema.store;

import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.exception.SchemaNotFoundException;

import java.util.Map;
import java.util.function.Predicate;

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

  public static boolean hasAlias(OrgMetricMetadata metricMetadata, String alias){
    return metricMetadata.getAliasValues().contains(alias);
  }

  static Predicate<Map.Entry<String, OrgMetricMetadata>> metricHasAlias(String name) {
    return nameToOrgMetric -> hasAlias(nameToOrgMetric.getValue(), name);
  }
}
