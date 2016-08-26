package io.fineo.lambda.handle.schema.metric.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;

import java.io.IOException;
import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.getModules;

/**
 * Wrapper to instantiate the raw stage
 */
public class ReadMetric
  extends LambdaResponseWrapper<ReadMetricRequest, ReadMetricResponse, ReadMetricHandler> {

  public ReadMetric() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public ReadMetric(List<Module> modules) {
    super(ReadMetricHandler.class, modules);
  }

  @Override
  public ReadMetricResponse handle(ReadMetricRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
