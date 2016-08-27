package io.fineo.lambda.handle.schema.metric.create;

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
public class CreateMetric extends
                          LambdaResponseWrapper<CreateMetricRequest, CreateMetricResponse,
                            CreateMetricHandler> {

  public CreateMetric() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public CreateMetric(List<Module> modules) {
    super(CreateMetricHandler.class, modules);
  }

  @Override
  public CreateMetricResponse handle(CreateMetricRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
