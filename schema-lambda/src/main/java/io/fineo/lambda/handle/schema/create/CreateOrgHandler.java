package io.fineo.lambda.handle.schema.create;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * A lambda handler that handles Kinesis events
 */
public class CreateOrgHandler implements
                                       RequestHandler<CreateOrgRequest, CreateOrgResponse> {
  @Override
  public CreateOrgResponse handleRequest(CreateOrgRequest input, Context context) {
    return null;
  }
}
