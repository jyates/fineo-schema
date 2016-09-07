package io.fineo.lambda.handle.schema.org.update;

import io.fineo.lambda.handle.schema.request.OrgRequest;

public class UpdateOrgRequest extends OrgRequest{

  private String[] metricTypeKeys;
  private String[] timestampPatterns;

  public String[] getMetricTypeKeys() {
    return metricTypeKeys;
  }

  public void setMetricTypeKeys(String[] metricTypeKeys) {
    this.metricTypeKeys = metricTypeKeys;
  }

  public String[] getTimestampPatterns() {
    return timestampPatterns;
  }

  public void setTimestampPatterns(String[] timestampPatterns) {
    this.timestampPatterns = timestampPatterns;
  }
}
