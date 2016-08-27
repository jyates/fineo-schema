package io.fineo.lambda.handle.schema.metric.delete;

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
public class DeleteMetric extends
                          LambdaResponseWrapper<DeleteMetricRequest, DeleteMetricResponse,
                            DeleteMetricHandler> {

  public DeleteMetric() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public DeleteMetric(List<Module> modules) {
    super(DeleteMetricHandler.class, modules);
  }

  @Override
  public DeleteMetricResponse handle(DeleteMetricRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
