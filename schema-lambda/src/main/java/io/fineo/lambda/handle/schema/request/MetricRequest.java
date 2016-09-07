package io.fineo.lambda.handle.schema.request;

public class MetricRequest extends OrgRequest {

  private String metricName;

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }
}
