package io.fineo.schema.store;

import com.google.common.annotations.VisibleForTesting;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.schema.Record;
import io.fineo.schema.exception.SchemaNotFoundException;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
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
    Optional<Map.Entry<String, String>> id =
      keys == null ?
      Optional.empty() :
      keys.entrySet().stream()
          .filter(entry -> record.getStringByField(entry.getKey()) != null)
          .findAny();

    String metricAlias;
    StoreClerk.Metric metric;
    // try just the simple metrictype key
    if (id.isPresent()) {
      metricAlias = record.getStringByField(id.get().getKey());
      metric = store.getMetricForCanonicalName(id.get().getValue());
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
    return new AvroSchemaEncoder(metadata.getMetadata().getCanonicalName(),
      rm.metric.getUnderlyingMetric(),
      rm.metricAlias, record);
  }

  @VisibleForTesting
  AvroSchemaEncoder getEncoderForTesting(String metricId, String metricName, Record record) {
    StoreClerk.Metric metric = store.getMetricForCanonicalName(metricId);
    return new AvroSchemaEncoder(metadata.getMetadata().getCanonicalName(),
      metric.getUnderlyingMetric(), metricName,
      record);
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
