package io.fineo.lambda.handle.schema.metric.update;

import io.fineo.lambda.handle.schema.MetricRequest;

public class UpdateMetricRequest extends MetricRequest{

  private String newDisplayName;
  private String[] aliases;

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
