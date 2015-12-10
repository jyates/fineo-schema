package io.fineo.schema.avro;

import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.Record;
import io.fineo.schema.store.SchemaBuilder;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bridge between the 'logical' schema and the physical schema.
 */
public class AvroSchemaBridge {

  private static final Log LOG = LogFactory.getLog(AvroSchemaBridge.class);
  private final Schema schema;
  // essentially the reverse of the alias map in the metric metadata
  private final Map<String, String> aliasToFieldMap = new HashMap<>();

  public AvroSchemaBridge(String orgid, Metric metric) {
    Schema.Parser parser = new Schema.Parser();
    parser.parse(metric.getMetricSchema());
    this.schema = parser.getTypes().get(
      SchemaUtils.getCustomerSchemaFullName(orgid, metric.getMetadata().getCanonicalName()));
    // build a map of the alias names -> schema names
    metric.getMetadata().getMetricTypes().getCanonicalNamesToAliases().entrySet().forEach(entry -> {
      entry.getValue().forEach(alias -> {
        aliasToFieldMap.putIfAbsent(alias, entry.getKey());
      });
    });
  }

  public GenericData.Record encode(Record record) {
    GenericData.Record avroRecord = new GenericData.Record(schema);
    // ignoring org and type, write the fields into the record
    for (Map.Entry<String, Object> fieldEntry : record.getFields()) {
      String key = fieldEntry.getKey();
      if (key.equals(SchemaBuilder.ORG_ID_KEY) || key.equals(SchemaBuilder.ORG_METRIC_TYPE_KEY)) {
        continue;
      }
      String fieldName = aliasToFieldMap.get(key);

      // add to the named field, if we have it
      if (fieldName != null) {
        avroRecord.put(fieldName, fieldEntry.getValue());
        continue;
      }
      // we have no idea what field this is, so track it under unknown fields
      Map<String, String> unknown = getAndSetUnknownFieldsIfEmpty(avroRecord);
      unknown.put(key, String.valueOf(fieldEntry.getValue()));
    }

    // ensure that we filled the 'default' fields
    getAndSetUnknownFieldsIfEmpty(avroRecord);
    return avroRecord;
  }

  private Map<String, String> getAndSetUnknownFieldsIfEmpty(GenericData.Record avroRecord) {
    Map<String, String> unknown = getUnknownFields(avroRecord);
    if (unknown == null) {
      unknown = new HashMap<>();
      avroRecord.put(SchemaBuilder.UNKNOWN_KEYS_FIELD, unknown);
    }
    return unknown;
  }

  private Map<String, String> getUnknownFields(GenericData.Record avroRecord) {
    return
      ((Map<String, String>) avroRecord.get(SchemaBuilder.UNKNOWN_KEYS_FIELD));
  }

  public static AvroSchemaBridge create(SchemaStore store, Record record) {
    String orgid = record.getStringByField(SchemaBuilder.ORG_ID_KEY);
    String type = record.getStringByField(SchemaBuilder.ORG_METRIC_TYPE_KEY);
    if (orgid == null || type == null) {
      return null;
    }

    Metadata orgMetadata = store.getSchemaTypes(orgid);
    if (orgMetadata == null) {
      LOG.info("No org found for id: " + orgid);
      return null;
    }

    // for each schema name (metric type) load the actual metric information
    Metric metric = null;
    for (Map.Entry<String, List<String>> metricNameAlias : orgMetadata.getMetricTypes()
                                                                      .getCanonicalNamesToAliases()
                                                                      .entrySet()) {
      // first alias set that matches
      if (metricNameAlias.getValue().contains(type)) {
        metric = store.getMetricMetadata(orgMetadata.getCanonicalName(), metricNameAlias.getKey());
        break;
      }
    }
    if (metric == null) {
      return null;
    }
    return new AvroSchemaBridge(orgMetadata.getCanonicalName(), metric);
  }
}
