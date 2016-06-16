package io.fineo.lambda.handle.schema.metric.update;

public class UpdateMetricRequest {

  private String orgId;
  private String userMetricName;
  private String newDisplayName;
  private String[] aliases;

  public String getOrgId() {
    return orgId;
  }

  public void setOrgId(String orgId) {
    this.orgId = orgId;
  }

  public String getUserMetricName() {
    return userMetricName;
  }

  public void setUserMetricName(String userMetricName) {
    this.userMetricName = userMetricName;
  }

  public String getNewDisplayName() {
    return newDisplayName;
  }

  public void setNewDisplayName(String newDisplayName) {
    this.newDisplayName = newDisplayName;
  }

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }
}
