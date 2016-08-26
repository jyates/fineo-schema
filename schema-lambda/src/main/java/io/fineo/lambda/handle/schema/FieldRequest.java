package io.fineo.lambda.handle.schema;

public class FieldRequest extends MetricRequest {

  private String fieldName;
  private String[] aliases;

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }
}
