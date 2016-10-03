package io.fineo.schema.store;

import com.google.common.annotations.VisibleForTesting;
import io.fineo.internal.customer.BaseFields;
import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.Metric;
import io.fineo.schema.FineoStopWords;
import io.fineo.schema.Record;
import io.fineo.schema.avro.SchemaNameUtils;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.timestamp.TimestampParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.time.Clock;
import java.time.Instant;
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
  private final String metricName;
  private final Record record;
  private final TimestampParser timestampParser;
  private final Metric metric;
  private Clock clock = Clock.systemUTC(); // same as instant.now()

  AvroSchemaEncoder(String canonicalOrgId, Metric metric, String metricName, Record record,
    TimestampParser timestampParser) {
    this.metric = metric;
    this.metricName = metricName;
    Schema.Parser parser = new Schema.Parser();
    parser.parse(metric.getMetricSchema());
    this.schema = parser.getTypes().get(
      SchemaNameUtils
        .getCustomerSchemaFullName(canonicalOrgId,
          metric.getMetadata().getMeta().getCanonicalName()));
    this.aliasToFieldMap = AvroSchemaManager.getAliasRemap(metric);
    this.record = record;
    this.timestampParser = timestampParser;
  }

  public GenericData.Record encode() {
    GenericData.Record avroRecord = new GenericData.Record(schema);
    // pull out the fields that all records must contain, the 'base' fields
    populateBaseFields(record, avroRecord);

    // copy over all the other fields that the schema knows about
    STOP.recordStart();
    for (Map.Entry<String, Object> entry : record.getFields()) {
      String key = entry.getKey();
      String fieldCName = aliasToFieldMap.get(key);
      // skip base/internal fields
      FieldMetadata fieldMetadata = metric.getMetadata().getFields().get(fieldCName);
      if (AvroSchemaProperties.IS_BASE_FIELD.test(key) ||
          (fieldMetadata != null && fieldMetadata.getInternalField())) {
        continue;
      }

      STOP.withField(key);

      if (fieldCName != null) {
        avroRecord
          .put(fieldCName, asTypedRecord(avroRecord.getSchema(), fieldCName, key, record));
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

  private void populateBaseFields(BaseFields baseMetricFields, Record record) {
    baseMetricFields.setTimestamp(getTimestamp(record));
    baseMetricFields.setWriteTime(Instant.now(clock).toEpochMilli());
    baseMetricFields.setAliasName(metricName);
  }

  // handle special case management of the timestamp
  private Long getTimestamp(Record record) {
    return timestampParser.getTimestamp(record);
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

  public static AvroSchemaEncoder create(SchemaStore store, Record record)
    throws SchemaNotFoundException {
    String orgid = record.getStringByField(AvroSchemaProperties.ORG_ID_KEY);
    return new StoreClerk(store, orgid).getEncoderFactory().getEncoder(record);
  }

  @VisibleForTesting
  void setClockForTesting(Clock clock) {
    this.clock = clock;
  }
}
