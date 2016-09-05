package io.fineo.schema.store;

import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.FineoStopWords;
import io.fineo.schema.Record;
import io.fineo.schema.avro.SchemaNameUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.HashMap;
import java.util.Map;

/**
 * Bridge between the 'logical' schema and the physical schema.
 * <p>
 * Not thread-safe.
 */
public class AvroSchemaEncoder {

  private final FineoStopWords STOP = new FineoStopWords();

  private final Schema schema;
  // essentially the reverse of the alias map in the metric metadata
  private final Map<String, String> aliasToFieldMap;
  private final OrgMetricMetadata metricMetadata;

  AvroSchemaEncoder(String canonicalOrgId, OrgMetricMetadata metricMetadata, Metric metric) {
    this.metricMetadata = metricMetadata;
    Schema.Parser parser = new Schema.Parser();
    parser.parse(metric.getMetricSchema());
    this.schema = parser.getTypes().get(
      SchemaNameUtils
        .getCustomerSchemaFullName(canonicalOrgId,
          metric.getMetadata().getMeta().getCanonicalName()));
    this.aliasToFieldMap = AvroSchemaManager.getAliasRemap(metric);
  }

  public GenericData.Record encode(Record record) {
    GenericData.Record avroRecord = new GenericData.Record(schema);
    // pull out the fields that all records must contain, the 'base' fields
    populateBaseFields(record, avroRecord);

    // copy over all the other fields that the schema knows about
    STOP.recordStart();
    for (Map.Entry<String, Object> entry : record.getFields()) {
      String key = entry.getKey();
      // org ID and canonical name is encoded in the schema
      if (AvroSchemaProperties.IS_BASE_FIELD.test(key)) {
        continue;
      }

      STOP.withField(key);
      String fieldName = aliasToFieldMap.get(key);

      // add to the named field, if we have it
      if (fieldName != null) {
        avroRecord
          .put(fieldName, asTypedRecord(avroRecord.getSchema(), fieldName, key, record));
        continue;
      }
      // we have no idea what field this is, so track it under unknown fields
      Map<String, String> unknown = getAndSetUnknownFieldsIfEmpty(avroRecord);
      unknown.put(key, String.valueOf(entry.getValue()));
    }

    STOP.endRecord();
    // ensure that we filled the 'default' fields
    getAndSetUnknownFieldsIfEmpty(avroRecord);
    return avroRecord;
  }

  public static GenericData.Record asTypedRecord(Schema objectSchema, String canonicalName,
    String recordFieldName, Record source) {
    Schema.Field field = objectSchema.getField(canonicalName);
    GenericData.Record record = new GenericData.Record(field.schema());
    record.put(AvroSchemaProperties.FIELD_INSTANCE_NAME, recordFieldName);
    Schema.Type type = field.schema().getField("value").schema().getType();
    Object value = null;
    switch (type) {
      case RECORD:
      case ENUM:
      case ARRAY:
      case MAP:
      case UNION:
      case FIXED:
        throw new IllegalArgumentException("Got nested event type: " + type);
      case STRING:
        value = source.getStringByField(recordFieldName);
        break;
      case BYTES:
        value = source.getBytesByFieldName(recordFieldName);
        break;
      case INT:
        value = source.getIntegerByField(recordFieldName);
        break;
      case LONG:
        value = source.getLongByFieldName(recordFieldName);
        break;
      case FLOAT:
        value = source.getFloatByFieldName(recordFieldName);
        break;
      case DOUBLE:
        value = source.getDoubleByFieldName(recordFieldName);
        break;
      case BOOLEAN:
        value = source.getBooleanByField(recordFieldName);
        break;
      case NULL:
        value = null;
        break;
    }
    record.put("value", value);
    return record;
  }

  private void populateBaseFields(Record record, GenericData.Record avroRecord) {
    BaseFields fields = (BaseFields) avroRecord.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    if (fields == null) {
      fields = new BaseFields();
      avroRecord.put(AvroSchemaProperties.BASE_FIELDS_KEY, fields);
    }
    populateBaseFields(fields, record);
  }

  private void populateBaseFields(BaseFields fields, Record record) {
    fields.setTimestamp(getTimestamp(record));
    // try all the user specified names first
    String aliasName = null;
    if (metricMetadata.getAliasKeys() != null) {
      for (String name : metricMetadata.getAliasKeys()) {
        aliasName = record.getStringByField(name);
        if (aliasName != null) {
          break;
        }
      }
    }
    // there are no alias key mappings that match, so just try the fineo key
    if (aliasName == null) {
      aliasName = record.getStringByField(AvroSchemaProperties.ORG_METRIC_TYPE_KEY);
    }
    fields.setAliasName(aliasName);
  }

  // handle special case management of the timestamp
  private Long getTimestamp(Record record) {
    try {
      return record.getLongByFieldName(AvroSchemaProperties.TIMESTAMP_KEY);
    } catch (ClassCastException e) {
      if (e.getMessage().contains("java.lang.Integer cannot be cast to java.lang.Long")) {
        return (long) record.getIntegerByField(AvroSchemaProperties.TIMESTAMP_KEY);
      }
      throw e;
    }
  }

  private Map<String, String> getAndSetUnknownFieldsIfEmpty(GenericData.Record avroRecord) {
    BaseFields fields = (BaseFields) avroRecord.get(AvroSchemaProperties.BASE_FIELDS_KEY);
    Map<String, String> unknown = fields.getUnknownFields();
    if (unknown == null) {
      unknown = new HashMap<>();
      fields.setUnknownFields(unknown);
    }
    return unknown;
  }

  public static AvroSchemaEncoder create(SchemaStore store, Record record) {
    String orgid = record.getStringByField(AvroSchemaProperties.ORG_ID_KEY);
    String type = record.getStringByField(AvroSchemaProperties.ORG_METRIC_TYPE_KEY);
    return new AvroSchemaManager(store, orgid).encode(type);
  }
}
