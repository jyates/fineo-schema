package io.fineo.lambda.handle.schema.metric.field;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * A lambda handler that handles Kinesis events
 */
public class AddFieldToMetricHandler
  implements RequestHandler<AddFieldToMetricRequest, AddFieldToMetricResponse> {


  @Override
  public AddFieldToMetricResponse handleRequest(AddFieldToMetricRequest input, Context context) {
    return null;
  }
}
