package io.fineo.lambda.handle.schema.org;

import io.fineo.lambda.handle.schema.org.read.ReadOrgRequest;
import io.fineo.lambda.handle.schema.request.OrgRequest;
import io.fineo.lambda.handle.schema.org.update.UpdateOrgRequest;

/**
 *
 */
public class ExternalOrgRequest extends OrgRequest{

  private String type;
  private UpdateOrgRequest patch;
  private ReadOrgRequest get;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public UpdateOrgRequest getPatch() {
    return patch;
  }

  public void setPatch(UpdateOrgRequest patch) {
    this.patch = patch;
  }

  public ReadOrgRequest getGet() {
    return get;
  }

  public void setGet(ReadOrgRequest get) {
    this.get = get;
  }
}
