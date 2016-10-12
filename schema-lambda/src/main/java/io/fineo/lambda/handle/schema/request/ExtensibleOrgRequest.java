package io.fineo.lambda.handle.schema.request;

public class ExtensibleOrgRequest<BODY> {

  private BODY body;
  private String orgId;

  public String getOrgId() {
    return orgId;
  }

  public <T extends ExtensibleOrgRequest> T setOrgId(String orgId) {
    this.orgId = orgId;
    return (T) this;
  }
}
