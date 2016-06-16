package io.fineo.lambda.handle.schema.metric.field;

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
public class AddFieldToMetric extends
                              LambdaResponseWrapper<AddFieldToMetricRequest,
                                AddFieldToMetricResponse, AddFieldToMetricHandler> {

  public AddFieldToMetric() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public AddFieldToMetric(List<Module> modules) {
    super(AddFieldToMetricHandler.class, modules);
  }


  @VisibleForTesting
  public static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    // add more Guice modules here
    return modules;
  }

  @Override
  public AddFieldToMetricResponse handle(AddFieldToMetricRequest input,
    Context context) throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
