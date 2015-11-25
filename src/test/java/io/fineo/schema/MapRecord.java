package io.fineo.schema;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class MapRecord implements Record {
  private final Map<String, Object> map;

  public MapRecord(Map<String, Object> fields) {
    this.map = fields;
  }

  @Override
  public Boolean getBooleanByField(String fieldName) {
    return (Boolean) map.get(fieldName);
  }

  @Override
  public Integer getIntegerByField(String fieldName) {
    return (Integer) map.get(fieldName);
  }

  @Override
  public Long getLongByFieldName(String fieldName) {
    return (Long) map.get(fieldName);
  }

  @Override
  public Float getFloatByFieldName(String fieldName) {
    return (Float) map.get(fieldName);
  }

  @Override
  public Double getDoubleByFieldName(String fieldName) {
    return (Double) map.get(fieldName);
  }

  @Override
  public byte[] getBytesByFieldName(String fieldName) {
    return (byte[]) map.get(fieldName);
  }

  @Override
  public String getStringByField(String fieldName) {
    return (String) map.get(fieldName);
  }

  @Override
  public Collection<String> getFieldNames() {
    return map.keySet();
  }

  @Override
  public Iterable<Map.Entry<String, Object>> getFields() {
    return map.entrySet();
  }
}
