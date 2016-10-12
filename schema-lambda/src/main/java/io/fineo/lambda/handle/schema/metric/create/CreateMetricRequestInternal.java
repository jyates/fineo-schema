package io.fineo.lambda.handle.schema.metric.create;

import io.fineo.client.model.schema.metric.CreateMetricRequest;
import io.fineo.lambda.handle.schema.request.OrgRequest;

public class CreateMetricRequestInternal extends OrgRequest {

  private CreateMetricRequest body;

  public CreateMetricRequest getBody() {
    return body;
  }

  public void setBody(CreateMetricRequest body) {
    this.body = body;
  }
}
