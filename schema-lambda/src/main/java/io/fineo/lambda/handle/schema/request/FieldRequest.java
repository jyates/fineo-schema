package io.fineo.lambda.handle.schema.request;

public class FieldRequest extends MetricRequest {

  private String fieldName;

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }
}