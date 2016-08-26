package io.fineo.lambda.handle.schema.metric.read;

import io.fineo.lambda.handle.schema.field.read.ReadFieldResponse;

public class ReadMetricResponse {
  public String name;
  public String[] aliases;
  public ReadFieldResponse[] fields;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String[] getAliases() {
    return aliases;
  }

  public void setAliases(String[] aliases) {
    this.aliases = aliases;
  }

  public ReadFieldResponse[] getFields() {
    return fields;
  }

  public void setFields(ReadFieldResponse[] fields) {
    this.fields = fields;
  }
}
