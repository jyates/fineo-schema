package io.fineo.schema;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * Simple {@link Record} that is directly backed by a {@link Map}
 */
public class MapRecord implements Record {
  private final Map<String, Object> map;

  public MapRecord(Map<String, Object> fields) {
    this.map = fields;
  }

  @Override
  public Boolean getBooleanByField(String fieldName) {
    Object o = map.get(fieldName);
    if (o instanceof String) {
      return Boolean.valueOf((String) o);
    }
    return (Boolean) o;
  }

  @Override
  public Integer getIntegerByField(String fieldName) {
    Object o = map.get(fieldName);
    if (o instanceof String) {
      return Integer.valueOf((String) o);
    }
    return (Integer) o;
  }

  @Override
  public Long getLongByFieldName(String fieldName) {
    Object o = map.get(fieldName);
    if (o instanceof String) {
      return Long.valueOf((String) o);
    }
    return (Long) o;
  }

  @Override
  public Float getFloatByFieldName(String fieldName) {
    Object o = map.get(fieldName);
    if (o instanceof String) {
      return Float.valueOf((String) o);
    }
    return (Float) o;
  }

  @Override
  public Double getDoubleByFieldName(String fieldName) {
    Object o = map.get(fieldName);
    if (o instanceof String) {
      return Double.valueOf((String) o);
    }
    return (Double) o;
  }

  @Override
  public ByteBuffer getBytesByFieldName(String fieldName) {
    return (ByteBuffer) map.get(fieldName);
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

  @Override
  public Object getField(String name) {
    return map.get(name);
  }

  @Override
  public String toString() {
    return this.map.toString();
  }
}
