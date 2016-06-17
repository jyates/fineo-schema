package io.fineo.schema;

import io.fineo.internal.customer.Metric;

/**
 * Marker exception indicating that the schema you are trying to update has already been updated.
 */
public class OldSchemaException extends Exception {

  public OldSchemaException(Metric storedPrevious, Metric expectedPrevious) {
    super("Have non-matching previous metric!\nStored:   " + storedPrevious + " \nvs.\nExpected: " +
    expectedPrevious);
  }
}
