package io.fineo.lambda.handle.schema.org.read;

import io.fineo.lambda.handle.schema.response.OrgResponse;

public class ReadOrgResponse extends OrgResponse {
  private String[] timestampPatterns;
  private String [] metricKeys;

  public String[] getTimestampPatterns() {
    return timestampPatterns;
  }

  public void setTimestampPatterns(String[] timestampPatterns) {
    this.timestampPatterns = timestampPatterns;
  }

  public String[] getMetricKeys() {
    return metricKeys;
  }

  public void setMetricKeys(String[] metricKeys) {
    this.metricKeys = metricKeys;
  }
}
