package io.fineo.lambda.handle.schema.metric.update;

import io.fineo.client.model.schema.metric.UpdateMetricRequest;
import io.fineo.lambda.handle.schema.request.OrgRequest;

public class UpdateMetricRequestInternal extends OrgRequest {

  private UpdateMetricRequest body;

  public UpdateMetricRequest getBody() {
    return body;
  }

  public void setBody(UpdateMetricRequest body) {
    this.body = body;
  }
}
