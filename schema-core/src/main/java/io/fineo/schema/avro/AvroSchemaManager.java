package io.fineo.schema.avro;

import com.google.common.base.Preconditions;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manage translating to/from an Avro {@link org.apache.avro.generic.GenericRecord} and a simple
 * {@link io.fineo.schema.Record}
 * @deprecated Use {@link StoreClerk instead}
 */
@Deprecated
public class AvroSchemaManager {

  private final SchemaStore store;
  private final Metadata orgMetadata;

  public AvroSchemaManager(SchemaStore store, String orgId) {
    this.store = store;
    this.orgMetadata =
      checkNotNull(store.getOrgMetadata(checkNotNull(orgId, "OrgID can never be null!")));
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

  public Metadata getOrgMetadata(){
    return this.orgMetadata;
  }

  public static AvroRecordTranslator translator(SchemaStore store, GenericRecord record){
    RecordMetadata metadata = RecordMetadata.get(record);
    return new AvroSchemaManager(store, metadata.getOrgID()).translator(record);
  }

  public AvroRecordTranslator translator(GenericRecord record) {
    return new AvroRecordTranslator(record, store);
  }

  public static Map<String, String> getAliasRemap(Metadata metadata){
    Map<String, String> aliasToFieldMap = new HashMap<>();
    metadata.getCanonicalNamesToAliases().entrySet().stream()
          .forEach(entry ->
            entry.getValue().forEach(alias -> aliasToFieldMap.putIfAbsent(alias, entry.getKey())));
    return aliasToFieldMap;
  }

  public static Map<String, String> getAliasRemap(Metric metric) {
    // build a map of the alias names -> schema names (reverse of the canonical -> alias map we
    // keep in the schema)
    // essentially the reverse of the alias map in the metric metadata
    return getAliasRemap(metric.getMetadata());
  }
}
