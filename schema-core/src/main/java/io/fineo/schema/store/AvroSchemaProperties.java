package io.fineo.schema.store;

import io.fineo.schema.ClientSchemaProperties;

import java.util.function.Predicate;

public class AvroSchemaProperties {
  /**
   * Single place that we reference the schema names for the base fields, so we can set/extract
   * them by name properly
   */
  public static final String ORG_ID_KEY = "companykey";
  public static final String ORG_METRIC_TYPE_KEY = ClientSchemaProperties.METRIC_TYPE_KEY;
  public static final String TIMESTAMP_KEY = ClientSchemaProperties.TIMESTAMP_KEY;
  public static final String WRITE_TIME_KEY = "writeTime";
  public static final String FIELD_INSTANCE_NAME = "displayName";
  public static final String METRIC_ORIGINAL_FIELD_ALIAS = "aliasName";

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
  /**
   * name in the base schema that contains the metrics that all records must have
   */
  public static final String BASE_FIELDS_KEY = "baseFields";
}
