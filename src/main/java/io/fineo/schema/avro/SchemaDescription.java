package io.fineo.schema.avro;

import javafx.util.Pair;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapper around an avro {@link org.apache.avro.Schema} with helper methods to store the
 * schema and access fields
 */
public class SchemaDescription {

  // canonical name for the schema, verified to be unique on the backend
  private final String schemaName;

  // alias names for the canonical scehema name
  private final List<String> schemaNames;
  // generic ID to identify the schema on the server
  private String id;
  private String subjectName;// aka customer id
  private Schema schema;
  private Map<String, Schema.Field> fieldAliasMap = new HashMap<>();

  public SchemaDescription(Schema schema, List<String> schemaNames,
    List<Pair<String, String>> fieldNames) {
    this.schema = schema;
    this.subjectName = schema.getNamespace();
    this.schemaName = schema.getName();
    this.schemaNames = schemaNames;
    for (Pair<String, String> field : fieldNames) {
      fieldAliasMap.put(field.getKey(), schema.getField(field.getValue()));
    }
  }

  public String getSubjectName() {
    return this.subjectName;
  }

  public Schema getSchema() {
    return this.schema;
  }

  public void setId(String id) {
    this.id = id;
  }
}
