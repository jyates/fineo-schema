package io.fineo.schema.store;

import io.fineo.internal.customer.MetricMetadata;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.Record;
import io.fineo.schema.exception.SchemaNotFoundException;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class SchemaUtils {

  private SchemaUtils() {
  }

  public static String getCanonicalName(MetricMetadata metadata, String aliasName) {
    Map<String, String> aliasToCname = AvroSchemaManager.getAliasRemap(metadata);
    return aliasToCname.get(aliasName);
  }

  public static String getCanonicalName(OrgMetadata metadata, String aliasName) {
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

  public static boolean hasAlias(OrgMetricMetadata metricMetadata, String alias) {
    return metricMetadata.getAliasValues().contains(alias);
  }

  static Predicate<Map.Entry<String, OrgMetricMetadata>> metricHasAlias(String name) {
    return nameToOrgMetric -> hasAlias(nameToOrgMetric.getValue(), name);
  }

  public static Optional<String> getFieldInRecord(Record record, Collection<String> possibleNames) {
    return possibleNames == null ?
           Optional.empty() :
           possibleNames.stream()
                        .filter(name -> record.getField(name) != null)
                        .findAny();
  }
}
