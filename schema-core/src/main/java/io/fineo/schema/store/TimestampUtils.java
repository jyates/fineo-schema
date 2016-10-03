package io.fineo.schema.store;

import io.fineo.schema.timestamp.TimestampFieldExtractor;

public class TimestampUtils {

  private TimestampUtils() {
  }

  public static TimestampFieldExtractor createExtractor(StoreClerk.Metric metric) {
    return new TimestampFieldExtractor(metric.getTimestampField().getAliases());
  }
}
