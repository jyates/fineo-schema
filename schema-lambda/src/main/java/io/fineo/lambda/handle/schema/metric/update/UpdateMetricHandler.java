package io.fineo.lambda.handle.schema.metric.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.fineo.lambda.handle.schema.field.UpdateFieldResponse;

public class UpdateMetricHandler
  implements RequestHandler<UpdateMetricRequest, UpdateMetricResponse> {

  @Override
  public UpdateMetricResponse handleRequest(UpdateMetricRequest input, Context context) {
    return null;
  }
}
