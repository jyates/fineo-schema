package io.fineo.lambda.handle.schema.org.metrics;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.ThrowingSupplier;
import io.fineo.lambda.handle.schema.org.RequestRunner;
import io.fineo.lambda.handle.schema.org.read.ReadOrgRequest;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;

import java.util.HashMap;
import java.util.Map;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;

/**
 * Do the work of reading the ids and the metric names for the given org
 */
public class ReadOrgMetricsHandler
  extends ExternalFacingRequestHandler<ReadOrgRequest, ReadOrgMetricsResponse> {

  private final Provider<SchemaStore> store;
  private final RequestRunner runner;

  @Inject
  public ReadOrgMetricsHandler(Provider<SchemaStore> store, RequestRunner runner) {
    this.store = store;
    this.runner = runner;
  }

  @Override
  public ReadOrgMetricsResponse handle(ReadOrgRequest input, Context
    context) throws Exception {
    validateRequest(context, input);
    return runner.run(getRequestHandler(input, context), context);
  }

  private ThrowingSupplier<ReadOrgMetricsResponse> getRequestHandler(ReadOrgRequest input, Context
    context) throws JsonProcessingException{
    return () -> {
      StoreClerk clerk = new StoreClerk(store.get(), input.getOrgId());
      ReadOrgMetricsResponse response = new ReadOrgMetricsResponse();
      response.setIdToMetricName(clerk.getMetricIdsToNames());
      return response;
    };
  }
}
