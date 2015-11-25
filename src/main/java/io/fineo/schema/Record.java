package io.fineo.schema;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Adapter to a record instance to pass through reading values
 */
public interface Record {

  Boolean getBooleanByField(String fieldName);

  Integer getIntegerByField(String fieldName);

  Long getLongByFieldName(String fieldName);

  Float getFloatByFieldName(String fieldName);

  Double getDoubleByFieldName(String fieldName);

  byte[] getBytesByFieldName(String fieldName);

  String getStringByField(String fieldName);

  Collection<String> getFieldNames();

  Iterable<Map.Entry<String, Object>> getFields();
}
