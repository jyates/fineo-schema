package io.fineo.lambda.handle.schema;

public class FieldRequest extends MetricRequest {

  private String fieldName;

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }
}
