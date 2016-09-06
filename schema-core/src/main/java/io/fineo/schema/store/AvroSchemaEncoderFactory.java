package io.fineo.schema.store;

import io.fineo.internal.customer.OrgMetadata;
import io.fineo.schema.Record;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.timestamp.MultiPatternTimestampParser;
import io.fineo.schema.store.timestamp.TimestampFieldExtractor;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Factory to around the encoding of a record
 */
public class AvroSchemaEncoderFactory {

  private final StoreClerk store;
  private final OrgMetadata metadata;

  public AvroSchemaEncoderFactory(StoreClerk clerk, OrgMetadata orgMetadata) {
    this.store = clerk;
    this.metadata = orgMetadata;
  }

  public RecordMetric getMetricForRecord(Record record) throws SchemaNotFoundException {
    Map<String, String> keys = metadata.getMetricKeyMap();
    Optional<String> key = keys == null ? Optional.empty() : SchemaUtils.getFieldInRecord(record,
      keys.keySet());

    String metricAlias;
    StoreClerk.Metric metric;
    // try just the simple metrictype key
    if (key.isPresent()) {
      metricAlias = record.getStringByField(key.get());
      metric = store.getMetricForCanonicalName(keys.get(key.get()));
    } else {
      metricAlias = checkNotNull(record.getStringByField(AvroSchemaProperties.ORG_METRIC_TYPE_KEY),
        "No metric type found in record for fields %s or standard key %s",
        keys == null ? "[]" : keys.keySet(),
        AvroSchemaProperties.ORG_METRIC_TYPE_KEY);
      metric = store.getMetricForUserNameOrAlias(metricAlias);
    }
    return new RecordMetric(metricAlias, metric);
  }

  public AvroSchemaEncoder getEncoder(Record record)
    throws SchemaNotFoundException {
    RecordMetric rm = getMetricForRecord(record);
    MultiPatternTimestampParser parser =
      new MultiPatternTimestampParser(metadata, rm.metric.getTimestampPatterns(),
        TimestampFieldExtractor.create(rm.metric));
    return new AvroSchemaEncoder(metadata.getMetadata().getCanonicalName(),
      rm.metric.getUnderlyingMetric(),
      rm.metricAlias, record, parser);
  }

  public static class RecordMetric {
    public final String metricAlias;
    public final StoreClerk.Metric metric;

    public RecordMetric(String metricAlias, StoreClerk.Metric metric) {
      this.metricAlias = metricAlias;
      this.metric = metric;
    }
  }
}
