package io.fineo.lambda.handle.schema.metric.field;

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
public class AddFieldToMetric extends
                              LambdaResponseWrapper<AddFieldToMetricRequestInternal,
                                AddFieldToMetricResponse, AddFieldToMetricHandler> {

  public AddFieldToMetric() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public AddFieldToMetric(List<Module> modules) {
    super(AddFieldToMetricHandler.class, modules);
  }

  @Override
  public AddFieldToMetricResponse handle(AddFieldToMetricRequestInternal input,
    Context context) throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
