package io.fineo.lambda.handle.schema.request;

/**
 *
 */
public class OrgRequest {

  private String orgId;

  public String getOrgId() {
    return orgId;
  }

  public <T extends OrgRequest> T setOrgId(String orgId) {
    this.orgId = orgId;
    return (T) this;
  }
}
