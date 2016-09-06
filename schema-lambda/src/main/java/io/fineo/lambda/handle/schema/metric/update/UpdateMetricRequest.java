package io.fineo.lambda.handle.schema.metric.update;

import io.fineo.lambda.handle.schema.MetricRequest;

public class UpdateMetricRequest extends MetricRequest{

  private String[] newKeys;
  private String[] removeKeys;
  private String newDisplayName;
  private String[] aliases;
  private String[] timestampPatterns;

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

  public String[] getNewKeys() {
    return newKeys;
  }

  public void setNewKeys(String[] newKeys) {
    this.newKeys = newKeys;
  }

  public String[] getRemoveKeys() {
    return removeKeys;
  }

  public void setRemoveKeys(String[] removeKeys) {
    this.removeKeys = removeKeys;
  }

  public String[] getTimestampPatterns() {
    return timestampPatterns;
  }

  public void setTimestampPatterns(String[] timestampPatterns) {
    this.timestampPatterns = timestampPatterns;
  }
}
