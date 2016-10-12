package io.fineo.lambda.handle.schema.metric.create;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.client.model.schema.metric.CreateMetricRequest;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateMetricRequest;
import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;

public class CreateMetricHandler
  extends ExternalFacingRequestHandler<CreateMetricRequestInternal, CreateMetricResponse> {

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
  public CreateMetricResponse handle(CreateMetricRequestInternal irequest, Context context)
    throws Exception {
    validateRequest(context, irequest);

    CreateMetricRequest request = irequest.getBody();
    validateMetricRequest(context, request);

    return retry.run(() -> {
      StoreManager manager = store.get();
      manager.updateOrg(irequest.getOrgId())
             .newMetric().setDisplayName(request.getMetricName()).addAliases(request.getAliases())
             .build().commit();
      return RESPONSE;
    }, context);
  }
}
