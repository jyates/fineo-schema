package io.fineo.lambda.handle.schema.org.metrics;

import io.fineo.lambda.handle.schema.response.OrgResponse;

import java.util.Map;

public class ReadOrgMetricsResponse extends OrgResponse {
  private Map<String, String> idToMetricName;

  public Map<String, String> getIdToMetricName() {
    return idToMetricName;
  }

  public void setIdToMetricName(Map<String, String> idToMetricName) {
    this.idToMetricName = idToMetricName;
  }
}
