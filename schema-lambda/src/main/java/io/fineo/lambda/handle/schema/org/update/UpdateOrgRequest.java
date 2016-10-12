package io.fineo.lambda.handle.schema.org.update;

import io.fineo.client.model.schema.SchemaManagementRequest;
import io.fineo.lambda.handle.schema.request.OrgRequest;

public class UpdateOrgRequest extends OrgRequest {

  private SchemaManagementRequest body;

  public SchemaManagementRequest getBody() {
    return body;
  }

  public void setBody(SchemaManagementRequest body) {
    this.body = body;
  }
}
