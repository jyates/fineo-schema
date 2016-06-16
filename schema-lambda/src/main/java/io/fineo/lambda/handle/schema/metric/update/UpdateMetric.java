package io.fineo.lambda.handle.schema.metric.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

  @VisibleForTesting
  public static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    // add more Guice modules here
    return modules;
  }

  @Override
  public UpdateMetricResponse handle(UpdateMetricRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
