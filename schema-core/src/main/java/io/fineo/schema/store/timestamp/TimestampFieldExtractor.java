package io.fineo.schema.store.timestamp;

import io.fineo.schema.Record;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaUtils;
import io.fineo.schema.store.StoreClerk;

import java.util.List;

/**
 * Extract the timestamp field based on the aliases for the field
 */
public class TimestampFieldExtractor {

  private final List<String> aliases;

  public static TimestampFieldExtractor create(StoreClerk.Metric metric) {
    StoreClerk.Field field = metric.getFieldForCanonicalName(AvroSchemaProperties.TIMESTAMP_KEY);
    return new TimestampFieldExtractor(field.getAliases());
  }

  private TimestampFieldExtractor(List<String> timestampAliases) {
    this.aliases = timestampAliases;
  }

  public String getTimestampKey(Record record) {
    return SchemaUtils.getFieldInRecord(record, aliases).orElse(AvroSchemaProperties.TIMESTAMP_KEY);
  }
}
