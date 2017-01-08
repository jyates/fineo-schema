package io.fineo.lambda.handle.schema.org.metrics;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;
import io.fineo.lambda.handle.schema.metric.create.CreateMetricHandler;
import io.fineo.lambda.handle.schema.metric.create.CreateMetricRequestInternal;
import io.fineo.lambda.handle.schema.metric.create.CreateMetricResponse;
import io.fineo.lambda.handle.schema.org.read.ReadOrgRequest;

import java.io.IOException;
import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.getModules;

/**
 *
 */
public class ReadOrgMetrics extends
                            LambdaResponseWrapper<ReadOrgRequest, ReadOrgMetricsResponse,
                              ReadOrgMetricsHandler> {

  public ReadOrgMetrics() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public ReadOrgMetrics(List<Module> modules) {
    super(ReadOrgMetricsHandler.class, modules);
  }

  @Override
  public ReadOrgMetricsResponse handle(ReadOrgRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
