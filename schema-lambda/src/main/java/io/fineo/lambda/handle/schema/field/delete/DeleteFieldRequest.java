package io.fineo.lambda.handle.schema.field.delete;

import io.fineo.lambda.handle.schema.request.MetricRequest;

public class DeleteFieldRequest extends MetricRequest{
  private String fieldName;

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }
}
