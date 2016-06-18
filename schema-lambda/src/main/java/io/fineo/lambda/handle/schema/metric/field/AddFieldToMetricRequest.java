package io.fineo.lambda.handle.schema.metric.field;

import io.fineo.lambda.handle.schema.FieldRequest;

public class AddFieldToMetricRequest extends FieldRequest {

  private String fieldType;

  public String getFieldType() {
    return fieldType;
  }

  public void setFieldType(String fieldType) {
    this.fieldType = fieldType;
  }
}
