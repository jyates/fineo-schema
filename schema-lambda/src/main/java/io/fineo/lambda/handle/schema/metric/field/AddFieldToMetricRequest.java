package io.fineo.lambda.handle.schema.metric.field;

public class AddFieldToMetricRequest {

  private String orgId;
  private String userMetricName;
  private String fieldName;
  private String fieldType;
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

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldType() {
    return fieldType;
  }

  public void setFieldType(String fieldType) {
    this.fieldType = fieldType;
  }

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }
}
