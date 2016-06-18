package io.fineo.lambda.handle.schema.field.delete;

import io.fineo.lambda.handle.schema.MetricRequest;

public class DeleteFieldRequest extends MetricRequest{
  private String userFieldName;

  public String getUserFieldName() {
    return userFieldName;
  }

  public void setUserFieldName(String userFieldName) {
    this.userFieldName = userFieldName;
  }
}
