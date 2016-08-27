package io.fineo.lambda.handle.schema.metric.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;

import java.io.IOException;
import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.getModules;

/**
 * Wrapper to instantiate the raw stage
 */
public class UpdateMetric extends
                          LambdaResponseWrapper<UpdateMetricRequest, UpdateMetricResponse, UpdateMetricHandler> {

  public UpdateMetric() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public UpdateMetric(List<Module> modules) {
    super(UpdateMetricHandler.class, modules);
  }

  @Override
  public UpdateMetricResponse handle(UpdateMetricRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
