package io.fineo.lambda.handle.schema.metric.create;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.ThrowingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.validateMetricRequest;

public class CreateMetricHandler
  extends ThrowingRequestHandler<CreateMetricRequest, CreateMetricResponse> {

  private static final CreateMetricResponse RESPONSE = new CreateMetricResponse();
  private final Provider<StoreManager> store;
  private final UpdateRetryer retry;

  @Inject
  public CreateMetricHandler(Provider<StoreManager> manager,
    UpdateRetryer retryer,
    @Named(SchemaStoreModule.SCHEMA_UPDATE_RETRIES) int retries) {
    this.store = manager;
    this.retry = retryer;
    this.retry.setRetries(retries);
  }


  @Override
  public CreateMetricResponse handle(CreateMetricRequest input, Context context) throws Exception {
    validateMetricRequest(input);
    return retry.run(() -> {
      StoreManager manager = store.get();
      manager.updateOrg(input.getOrgId())
             .newMetric().setDisplayName(input.getMetricUserName()).addAliases(input.getAliases())
             .build().commit();
      return RESPONSE;
    });
  }
}
