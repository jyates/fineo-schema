package io.fineo.schema.avro;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Manage translating to/from an Avro {@link org.apache.avro.generic.GenericRecord} and a simple
 * {@link io.fineo.schema.Record}
 */
public class AvroSchemaManager {

  private final SchemaStore store;
  private final Metadata orgMetadata;

  public AvroSchemaManager(SchemaStore store, String orgId) {
    Preconditions.checkNotNull(orgId);
    this.store = store;
    this.orgMetadata = store.getSchemaTypes(orgId);
    Preconditions.checkNotNull(orgMetadata);
  }

  public Metric getMetricInfo(String aliasMetricName){
    return store.getMetricMetadataFromAlias(orgMetadata, aliasMetricName);
  }

  public AvroSchemaEncoder encode(String aliasedMetricType) {
    Preconditions.checkArgument(aliasedMetricType != null);

    Metric metric = store.getMetricMetadataFromAlias(orgMetadata, aliasedMetricType);
    Preconditions.checkArgument(metric != null,
      "Don't have a metric with alias: " + aliasedMetricType + " for org: " + orgMetadata
        .getCanonicalName());
    return new AvroSchemaEncoder(orgMetadata.getCanonicalName(), metric);
  }


  public static AvroRecordDecoder decoder(SchemaStore store, GenericRecord record){
    RecordMetadata metadata = RecordMetadata.get(record);
    return new AvroSchemaManager(store, metadata.getOrgID()).decoder(record);
  }

  public AvroRecordDecoder decoder(GenericRecord record) {
    return new AvroRecordDecoder(record, store);
  }

  public static Map<String, String> getAliasRemap(Metric metric) {
    // build a map of the alias names -> schema names (reverse of the canonical -> alias map we
    // keep in the schema)
    // essentially the reverse of the alias map in the metric metadata
    Map<String, String> aliasToFieldMap = new HashMap<>();
    metric.getMetadata().getCanonicalNamesToAliases().entrySet().stream()
          .forEach(entry ->
            entry.getValue().forEach(alias -> aliasToFieldMap.putIfAbsent(alias, entry.getKey())));
    return aliasToFieldMap;
  }
}
