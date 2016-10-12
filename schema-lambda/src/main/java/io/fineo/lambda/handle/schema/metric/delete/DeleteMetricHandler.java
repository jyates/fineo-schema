package io.fineo.lambda.handle.schema.metric.delete;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateMetricRequest;

public class DeleteMetricHandler
  extends ExternalFacingRequestHandler<DeleteMetricRequestInternal, DeleteMetricResponse> {

  private static final DeleteMetricResponse RESPONSE = new DeleteMetricResponse();
  private final Provider<StoreManager> store;
  private final UpdateRetryer retry;

  @Inject
  public DeleteMetricHandler(Provider<StoreManager> manager,
    UpdateRetryer retryer,
    @Named(SchemaStoreModule.SCHEMA_UPDATE_RETRIES) int retries) {
    this.store = manager;
    this.retry = retryer;
    this.retry.setRetries(retries);
  }

  @Override
  public DeleteMetricResponse handle(DeleteMetricRequestInternal request, Context context)
    throws Exception {
    validateMetricRequest(context, request);
    return retry.run(() -> {
      StoreManager manager = store.get();
      manager.updateOrg(request.getOrgId()).deleteMetric(request.getBody().getMetricName())
             .commit();
      return RESPONSE;
    }, context);
  }
}
