package io.fineo.lambda.handle.schema.metric.delete;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.ThrowingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.validateMetricRequest;

public class DeleteMetricHandler
  extends ThrowingRequestHandler<DeleteMetricRequest, DeleteMetricResponse> {

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
  public DeleteMetricResponse handle(DeleteMetricRequest input, Context context) throws Exception {
    validateMetricRequest(input);
    return retry.run(() -> {
      StoreManager manager = store.get();
      manager.updateOrg(input.getOrgId()).deleteMetric(input.getMetricUserName()).commit();
      return RESPONSE;
    });
  }
}
