package io.fineo.lambda.handle.schema.metric.create;

import io.fineo.lambda.handle.schema.request.MetricRequest;

public class CreateMetricRequest extends MetricRequest{

  private String[] aliases;

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }
}
