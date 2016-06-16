package io.fineo.schema.avro;

import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.Record;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Bridge between the 'logical' schema and the physical schema.
 */
public class AvroSchemaEncoder {

  /**
   * Single place that we reference the schema names for the base fields, so we can set/extract
   * them by name properly
   */
  public static final String ORG_ID_KEY = "companykey";
  public static final String ORG_METRIC_TYPE_KEY = "metrictype";
  public static final String TIMESTAMP_KEY = "timestamp";
  /**
   * name in the base schema that contains the metrics that all records must have
   */
  public static final String BASE_FIELDS_KEY = "baseFields";
  public static final String BASE_TIMESTAMP_FIELD_NAME = "timestamp";

  private final Schema schema;
  // essentially the reverse of the alias map in the metric metadata
  private final Map<String, String> aliasToFieldMap;

  AvroSchemaEncoder(String canonicalOrgId, Metric metric) {
    Schema.Parser parser = new Schema.Parser();
    parser.parse(metric.getMetricSchema());
    this.schema = parser.getTypes().get(
      SchemaNameUtils
        .getCustomerSchemaFullName(canonicalOrgId, metric.getMetadata().getCanonicalName()));
    this.aliasToFieldMap = AvroSchemaManager.getAliasRemap(metric);
  }

  public GenericData.Record encode(Record record) {
    GenericData.Record avroRecord = new GenericData.Record(schema);
    // pull out the fields that all records must contain, the 'base' fields
    populateBaseFields(record, avroRecord);

    // copy over all the other fields that the schema knows about
    for (Map.Entry<String, Object> fieldEntry : record.getFields()) {
      String key = fieldEntry.getKey();
      // org ID and canonical name is encoded in the schema
      if (IS_BASE_FIELD.test(key)) {
        continue;
      }

      String fieldName = aliasToFieldMap.get(key);

      // add to the named field, if we have it
      if (fieldName != null) {
        avroRecord.put(fieldName,
          asTypedRecord(avroRecord.getSchema(), fieldName, key, fieldEntry.getValue()));
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

  public static GenericData.Record asTypedRecord(Schema objectSchema, String canonicalName,
    String aliasName, Object value) {
    Schema.Field field = objectSchema.getField(canonicalName);
    GenericData.Record record = new GenericData.Record(field.schema());
    record.put("fieldAliasName", aliasName);
    record.put("value", value);
    return record;
  }

  /**
   * Function to help skip past the base field names in a record's schema. Returns <tt>true</tt>
   * when a field is a 'base field'.
   */
  public static Predicate<String> IS_BASE_FIELD = fieldName -> {
    switch (fieldName) {
      case ORG_ID_KEY:
      case ORG_METRIC_TYPE_KEY:
      case TIMESTAMP_KEY:
        return true;
    }
    return false;
  };

  private void populateBaseFields(Record record, GenericData.Record avroRecord) {
    BaseFields fields = (BaseFields) avroRecord.get(BASE_FIELDS_KEY);
    if (fields == null) {
      fields = new BaseFields();
      avroRecord.put(BASE_FIELDS_KEY, fields);
    }
    populateBaseFields(fields, record);
  }

  private void populateBaseFields(BaseFields fields, Record record) {
    fields.setTimestamp(record.getLongByFieldName(TIMESTAMP_KEY));
    fields.setAliasName(record.getStringByField(ORG_METRIC_TYPE_KEY));
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

  public static AvroSchemaEncoder create(SchemaStore store, Record record) {
    String orgid = record.getStringByField(ORG_ID_KEY);
    String type = record.getStringByField(ORG_METRIC_TYPE_KEY);
    return new AvroSchemaManager(store, orgid).encode(type);
  }
}
