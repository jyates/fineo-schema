package io.fineo.lambda.handle.schema.request;

import io.fineo.client.model.schema.metric.MetricRequest;

public class MetricRequestInternal extends OrgRequest {

  private MetricRequest body;

  public MetricRequest getBody() {
    return body;
  }

  public void setBody(MetricRequest body) {
    this.body = body;
  }
}
