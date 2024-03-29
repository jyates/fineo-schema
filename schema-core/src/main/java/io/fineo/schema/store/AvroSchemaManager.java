package io.fineo.schema.store;

import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.MetricMetadata;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.schema.avro.RecordMetadata;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manage translating to/from an Avro {@link org.apache.avro.generic.GenericRecord} and a simple
 * {@link io.fineo.schema.Record}
 */
class AvroSchemaManager {

  private final SchemaStore store;

  public AvroSchemaManager(SchemaStore store, String orgId) {
    this.store = store;
    checkNotNull(store.getOrgMetadata(checkNotNull(orgId, "OrgID can never be null!")));
  }

  public static AvroRecordTranslator translator(SchemaStore store, GenericRecord record) {
    RecordMetadata metadata = RecordMetadata.get(record);
    return new AvroSchemaManager(store, metadata.getOrgID()).translator(record);
  }

  public AvroRecordTranslator translator(GenericRecord record) {
    return new AvroRecordTranslator(record, store);
  }

  public static Map<String, String> getAliasRemap(OrgMetadata org) {
    Map<String, String> aliasToFieldMap = new HashMap<>();
    org.getMetrics().entrySet().stream()
       .forEach(entry -> {
         for (String alias : entry.getValue().getAliasValues()) {
           aliasToFieldMap.put(alias, entry.getKey());
         }
       });
    return aliasToFieldMap;
  }

  public static Map<String, String> getAliasRemap(MetricMetadata metadata) {
    Map<String, String> aliasToFieldMap = new HashMap<>();
    metadata.getFields().entrySet().stream()
            .forEach(entry -> {
              for (String alias : entry.getValue().getFieldAliases()) {
                aliasToFieldMap.put(alias, entry.getKey());
              }
            });
    return aliasToFieldMap;
  }

  public static Map<String, String> getAliasRemap(Metric metric) {
    // build a map of the alias names -> schema names (reverse of the canonical -> alias map we
    // keep in the schema)
    // essentially the reverse of the alias map in the metric metadata
    return getAliasRemap(metric.getMetadata());
  }
}
