package io.fineo.lambda.handle.schema.metric.field;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Wrapper to instantiate the raw stage
 */
public class AddFieldToMetric extends LambdaResponseWrapper<AddFieldToMetricRequest, AddFieldToMetricResponse> {

  public AddFieldToMetric() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public AddFieldToMetric(List<Module> modules) {
    super(YOUR_HANDLER.class, modules);
  }

  @Override
  public void handle(YOUR_EVENT_TYPE event) throws IOException {
    getInstance().handle(event);
  }

  @VisibleForTesting
  public static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    // add more Guice modules here
    return modules;
  }
}
