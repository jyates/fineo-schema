package io.fineo.schema.store;

import io.fineo.schema.Record;
import io.fineo.schema.avro.RecordMetadata;
import io.fineo.schema.store.AvroSchemaEncoder;
import io.fineo.schema.store.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * Translate a record from the canonical names to the aliased names that a user understands. Does
 * not copy the record, but instead uses the record to get the underlying fields.
 */
public class AvroRecordTranslator {

  final RecordMetadata metadata;
  private final GenericRecord record;
  private final Map<String, String> aliasMap;

  AvroRecordTranslator(GenericRecord record, SchemaStore store) {
    this.record = record;
    this.metadata = RecordMetadata.get(record);
    this.aliasMap = AvroSchemaManager.getAliasRemap(store.getMetricMetadata(metadata));
  }

  public RecordMetadata getMetadata() {
    return this.metadata;
  }

  /**
   * Translate the remaining fields (non-base fields) in the record to the alias name. The
   * returned record's fields are queryable only by the alias names, not the canonical name
   */
  public Record getTranslatedRecord() {
    return new TranslatedRecord();
  }

  private class TranslatedRecord implements Record {

    @Override
    public Boolean getBooleanByField(String fieldName) {
      return (Boolean) getField(fieldName);
    }

    @Override
    public Integer getIntegerByField(String fieldName) {
      return (Integer) getField(fieldName);
    }

    @Override
    public Long getLongByFieldName(String fieldName) {
      return (Long) getField(fieldName);
    }

    @Override
    public Float getFloatByFieldName(String fieldName) {
      return (Float) getField(fieldName);
    }

    @Override
    public Double getDoubleByFieldName(String fieldName) {
      return (Double) getField(fieldName);
    }

    @Override
    public ByteBuffer getBytesByFieldName(String fieldName) {
      return (ByteBuffer) getField(fieldName);
    }

    @Override
    public String getStringByField(String fieldName) {
      return (String) getField(fieldName);
    }

    @Override
    public Collection<String> getFieldNames() {
      return aliasMap.keySet();
    }

    @Override
    public Iterable<Map.Entry<String, Object>> getFields() {
      return null;
    }

    @Override
    public Object getField(String aliasName) {
      String cname = aliasMap.get(aliasName);
      if (AvroSchemaEncoder.IS_BASE_FIELD.test(cname)) {
        return record.get(cname);
      }
      IndexedRecord field = (IndexedRecord) record.get(cname);
      return field.get(1);
    }
  }
}
