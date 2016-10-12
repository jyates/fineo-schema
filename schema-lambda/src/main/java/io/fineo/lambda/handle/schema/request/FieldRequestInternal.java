package io.fineo.lambda.handle.schema.request;

import io.fineo.client.model.schema.field.FieldRequest;

public class FieldRequestInternal extends MetricRequestInternal {

  private FieldRequest body;

  @Override
  public FieldRequest getBody() {
    return body;
  }

  public void setBody(FieldRequest body) {
    this.body = body;
  }
}
