package io.fineo.lambda.handle.schema.metric.field;

import io.fineo.lambda.handle.schema.request.FieldUpdateRequest;

public class AddFieldToMetricRequest extends FieldUpdateRequest {

  private String fieldType;

  public String getFieldType() {
    return fieldType;
  }

  public void setFieldType(String fieldType) {
    this.fieldType = fieldType;
  }
}
