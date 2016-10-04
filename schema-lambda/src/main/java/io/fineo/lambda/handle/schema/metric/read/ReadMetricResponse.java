package io.fineo.lambda.handle.schema.metric.read;

import io.fineo.lambda.handle.schema.field.read.ReadFieldResponse;

import java.util.Arrays;

public class ReadMetricResponse {
  public String name;
  public String[] aliases;
  public ReadFieldResponse[] fields;
  private String[] timestampPatterns;

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

  public void setTimestampPatterns(String[] timestampPatterns) {
    this.timestampPatterns = timestampPatterns;
  }

  public String[] getTimestampPatterns() {
    return timestampPatterns;
  }

  @Override
  public String toString() {
    return "ReadMetricResponse{" +
           "name='" + name + '\'' +
           ", aliases=" + Arrays.toString(aliases) +
           ", fields=" + Arrays.toString(fields) +
           ", timestampPatterns=" + Arrays.toString(timestampPatterns) +
           '}';
  }
}
