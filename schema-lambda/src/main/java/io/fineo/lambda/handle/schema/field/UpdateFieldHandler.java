package io.fineo.lambda.handle.schema.field;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * A lambda handler that handles Kinesis events
 */
public class UpdateFieldHandler implements RequestHandler<UpdateFieldRequest, UpdateFieldResponse> {

  @Override
  public UpdateFieldResponse handleRequest(UpdateFieldRequest input, Context context) {
    return null;
  }
}
