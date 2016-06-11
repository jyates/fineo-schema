package io.fineo.schema.exception;

import java.io.IOException;


public class SchemaNotFoundException extends IOException {
  public SchemaNotFoundException(String message) {
    super(message);
  }
}
