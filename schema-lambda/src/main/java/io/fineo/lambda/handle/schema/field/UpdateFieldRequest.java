package io.fineo.lambda.handle.schema.field;

public class UpdateFieldRequest {

  private String orgId;
  private String metricUserName;
  private String userFieldName;
  private String[] aliases;

  public String getOrgId() {
    return orgId;
  }

  public void setOrgId(String orgId) {
    this.orgId = orgId;
  }

  public String getMetricUserName() {
    return metricUserName;
  }

  public void setMetricUserName(String metricUserName) {
    this.metricUserName = metricUserName;
  }

  public String getUserFieldName() {
    return userFieldName;
  }

  public void setUserFieldName(String userFieldName) {
    this.userFieldName = userFieldName;
  }

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }
}
