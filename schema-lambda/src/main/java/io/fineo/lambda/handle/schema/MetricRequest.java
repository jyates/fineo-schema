package io.fineo.lambda.handle.schema;

public class MetricRequest extends OrgRequest {

  private String metricUserName;

  public String getMetricUserName() {
    return metricUserName;
  }

  public void setMetricUserName(String metricUserName) {
    this.metricUserName = metricUserName;
  }
}
