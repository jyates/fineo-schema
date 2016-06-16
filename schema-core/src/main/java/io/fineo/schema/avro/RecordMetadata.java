package io.fineo.schema.avro;

import io.fineo.internal.customer.BaseFields;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Metadata about a record that can tie it back to a particular org and metric
 */
public class RecordMetadata {
  private final GenericRecord record;
  String orgID;
  String metricCanonicalType;
  Schema metricSchema;

  private RecordMetadata(GenericRecord record) {
    this.record = record;
  }

  private RecordMetadata setOrgID(String orgID) {
    this.orgID = orgID;
    return this;
  }

  private RecordMetadata setMetricCanonicalType(String metricCanonicalType) {
    this.metricCanonicalType = metricCanonicalType;
    return this;
  }

  private RecordMetadata setMetricSchema(Schema metricSchema) {
    this.metricSchema = metricSchema;
    return this;
  }

  public String getOrgID() {
    return orgID;
  }

  public String getMetricCanonicalType() {
    return metricCanonicalType;
  }

  public Schema getMetricSchema() {
    return metricSchema;
  }

  public static RecordMetadata get(GenericRecord record) {
    Schema schema = record.getSchema();
    return new RecordMetadata(record).setMetricSchema(schema)
                               .setOrgID(SchemaNameUtils.getOrgId(schema.getNamespace()))
                               .setMetricCanonicalType(schema.getName());
  }

  public BaseFields getBaseFields() {
    Object obj = record.get(AvroSchemaEncoder.BASE_FIELDS_KEY);
    if (obj instanceof BaseFields) {
      return (BaseFields) obj;
    }
    GenericData.Record rbase = (GenericData.Record) obj;
    BaseFields fields = new BaseFields();
    fields.getSchema().getFields()
          .stream()
          .map(Schema.Field::name)
          .forEach(name -> fields.put(name, rbase.get(name)));
    return fields;
  }
}
