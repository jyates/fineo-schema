package io.fineo.lambda.handle.schema.metric.field;

import io.fineo.client.model.schema.field.CreateFieldRequest;
import io.fineo.lambda.handle.schema.request.OrgRequest;

public class AddFieldToMetricRequestInternal extends OrgRequest {

  private CreateFieldRequest body;

  public CreateFieldRequest getBody() {
    return body;
  }

  public void setBody(CreateFieldRequest body) {
    this.body = body;
  }
}
