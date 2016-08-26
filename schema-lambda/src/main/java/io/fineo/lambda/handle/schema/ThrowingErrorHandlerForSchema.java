package io.fineo.lambda.handle.schema;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.fineo.lambda.handle.ThrowingRequestHandler;
import io.fineo.lambda.handle.schema.inject.SchemaModulesUtil;

/**
 * Base class for schema manipulation lambda functions that can also throw exceptions
 */
public abstract class ThrowingErrorHandlerForSchema<INPUT, OUTPUT>
  extends ThrowingRequestHandler<INPUT, OUTPUT> {

  @Override
  public OUTPUT handleRequest(INPUT input, Context context) {
    try {
      return super.handleRequest(input, context);
    } catch (RuntimeException e) {
      // its already a json message, so just propagate it up
      if (e.getMessage().startsWith("{")) {
        throw e;
      }
      try {
        SchemaModulesUtil.throwError(context, 500, "Internal error", e.getMessage());
        throw new IllegalStateException("Should have had an Internal Error: " + e.getMessage());
      } catch (JsonProcessingException e1) {
        throw new RuntimeException(e1);
      }
    }
  }
}
