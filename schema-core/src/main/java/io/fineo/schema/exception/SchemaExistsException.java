package io.fineo.schema.exception;

import java.io.IOException;

/**
 *
 */
public class SchemaExistsException extends IOException{
  public SchemaExistsException(String message) {
    super(message);
  }
}
