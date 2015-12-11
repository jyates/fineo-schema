package io.fineo.schema.avro;

import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.Record;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bridge between the 'logical' schema and the physical schema.
 */
public class AvroSchemaBridge {

  /**
   * Single place that we reference the schema names for the base fields, so we can set/extract
   * them by name properly
   */
  private static final Log LOG = LogFactory.getLog(AvroSchemaBridge.class);

  public static final String ORG_ID_KEY = "companykey";
  public static final String ORG_METRIC_TYPE_KEY = "metrictype";
  public static final String TIMESTAMP_KEY = "timestamp";
  /** name in the base schema that contains the metrics that all records must have */
  public static final String BASE_FIELDS_KEY = "baseFields";
  public static final String BASE_TIMESTAMP_FIELD_NAME = "timestamp";


  private final Schema schema;
  // essentially the reverse of the alias map in the metric metadata
  private final Map<String, String> aliasToFieldMap = new HashMap<>();

  public AvroSchemaBridge(String orgid, Metric metric) {
    Schema.Parser parser = new Schema.Parser();
    parser.parse(metric.getMetricSchema());
    this.schema = parser.getTypes().get(
      SchemaNameUtils.getCustomerSchemaFullName(orgid, metric.getMetadata().getCanonicalName()));
    // build a map of the alias names -> schema names
    metric.getMetadata().getMetricTypes().getCanonicalNamesToAliases().entrySet().forEach(entry -> {
      entry.getValue().forEach(alias -> {
        aliasToFieldMap.putIfAbsent(alias, entry.getKey());
      });
    });
  }

  private void populateBaseFields(BaseFields fields, Record record) {
    fields.setTimestamp(record.getLongByFieldName(TIMESTAMP_KEY));
    fields.setAliasName(record.getStringByField(ORG_METRIC_TYPE_KEY));
  }

  public GenericData.Record encode(Record record) {
    GenericData.Record avroRecord = new GenericData.Record(schema);
    populateBaseFields(record, avroRecord);
    for (Map.Entry<String, Object> fieldEntry : record.getFields()) {
      String key = fieldEntry.getKey();
      // org ID and canonical name is encoded in the schema
      switch (key) {
        case ORG_ID_KEY:
        case ORG_METRIC_TYPE_KEY:
        case TIMESTAMP_KEY:
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

  private void populateBaseFields(Record record, GenericData.Record avroRecord) {
    BaseFields fields = (BaseFields) avroRecord.get(BASE_FIELDS_KEY);
    if (fields == null) {
      fields = new BaseFields();
      avroRecord.put(BASE_FIELDS_KEY, fields);
    }
    populateBaseFields(fields, record);
  }

  private Map<String, String> getAndSetUnknownFieldsIfEmpty(GenericData.Record avroRecord) {
    BaseFields fields = (BaseFields) avroRecord.get(BASE_FIELDS_KEY);
    Map<String, String> unknown = fields.getUnknownFields();
    if (unknown == null) {
      unknown = new HashMap<>();
      fields.setUnknownFields(unknown);
    }
    return unknown;
  }

  public static AvroSchemaBridge create(SchemaStore store, Record record) {
    String orgid = record.getStringByField(ORG_ID_KEY);
    String type = record.getStringByField(ORG_METRIC_TYPE_KEY);
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
