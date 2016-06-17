package io.fineo.lambda.handle.schema;

public class FieldRequest extends MetricRequest {

  private String userFieldName;
  private String[] aliases;

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
