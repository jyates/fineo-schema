package io.fineo.schema.avro;

import io.fineo.internal.customer.BaseFields;
import io.fineo.schema.Record;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Decode a record that has been encoded using the {@link AvroSchemaBridge}.
 */
public class AvroRecordDecoder {

  private final RecordMetadata metadata;
  private final GenericRecord record;

  public static class RecordMetadata {
    String orgID;
    String metricCannonicalType;
    Schema metricSchema;

    public RecordMetadata setOrgID(String orgID) {
      this.orgID = orgID;
      return this;
    }

    public RecordMetadata setMetricCannonicalType(String metricCannonicalType) {
      this.metricCannonicalType = metricCannonicalType;
      return this;
    }

    public RecordMetadata setMetricSchema(Schema metricSchema) {
      this.metricSchema = metricSchema;
      return this;
    }
  }

  public AvroRecordDecoder(GenericRecord record) {
    this.record = record;
    this.metadata = getMetadata(record);
  }

  public RecordMetadata getMetadata() {
    return this.metadata;
  }

  public BaseFields getBaseFields() {
    GenericData.Record rbase = (GenericData.Record) record.get(AvroSchemaBridge.BASE_FIELDS_KEY);
    BaseFields fields = new BaseFields();
    fields.getSchema().getFields()
          .stream()
          .map(Schema.Field::name)
          .forEach(name -> fields.put(name, rbase.get(name)));
    return fields;
  }

  private RecordMetadata getMetadata(GenericRecord record) {
    Schema schema = record.getSchema();
    return new RecordMetadata().setMetricSchema(schema)
                               .setOrgID(SchemaNameUtils.getOrgId(schema.getNamespace()))
                               .setMetricCannonicalType(schema.getName());
  }

  private class RecordMap implements Record {
    private final GenericData.Record base;

    public RecordMap(GenericData.Record base) {
      this.base = base;
    }

    @Override
    public Boolean getBooleanByField(String fieldName) {
      return (Boolean) base.get(fieldName);
    }

    @Override
    public Integer getIntegerByField(String fieldName) {
      return (Integer) base.get(fieldName);
    }

    @Override
    public Long getLongByFieldName(String fieldName) {
      return (Long) base.get(fieldName);
    }

    @Override
    public Float getFloatByFieldName(String fieldName) {
      return (Float) base.get(fieldName);
    }

    @Override
    public Double getDoubleByFieldName(String fieldName) {
      return (Double) base.get(fieldName);
    }

    @Override
    public ByteBuffer getBytesByFieldName(String fieldName) {
      return (ByteBuffer) base.get(fieldName);
    }

    @Override
    public String getStringByField(String fieldName) {
      return null;
    }

    @Override
    public Collection<String> getFieldNames() {
      return base.getSchema().getFields().stream().map(Schema.Field::name)
                 .collect(Collectors.toList());
    }

    @Override
    public Iterable<Map.Entry<String, Object>> getFields() {
      return null;
    }

    @Override
    public Object getField(String name) {
      return base.get(name);
    }
  }
}
