package io.fineo.lambda.handle.schema.field.update;

import io.fineo.client.model.schema.field.UpdateFieldRequest;
import io.fineo.lambda.handle.schema.request.OrgRequest;

public class UpdateFieldRequestInternal extends OrgRequest {
  private UpdateFieldRequest body;

  public UpdateFieldRequest getBody() {
    return body;
  }

  public void setBody(UpdateFieldRequest body) {
    this.body = body;
  }
}
