package io.fineo.schema.exception;

import java.io.IOException;

/**
 *
 */
public class SchemaTypeNotFoundException extends IOException {
  public SchemaTypeNotFoundException(String msg) {
    super(msg);
  }
}
