package io.fineo.schema.store.timestamp;

import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.MetricMetadata;
import io.fineo.schema.Record;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaUtils;

import java.util.List;

/**
 * Extract the timestamp field based on the aliases for the field
 */
public class TimestampFieldExtractor {

  private final MetricMetadata metric;

  public TimestampFieldExtractor(MetricMetadata metric) {
    this.metric = metric;
  }

  public String getTimestampKey(Record record) {
    FieldMetadata timestampMetadata = metric.getFields().get(AvroSchemaProperties.TIMESTAMP_KEY);
    List<String> aliases = timestampMetadata.getFieldAliases();
    return SchemaUtils.getFieldInRecord(record, aliases).orElse(AvroSchemaProperties.TIMESTAMP_KEY);
  }
}
