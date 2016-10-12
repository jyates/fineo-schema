package io.fineo.lambda.handle.schema.metric.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.client.model.schema.metric.UpdateMetricRequest;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateMetricRequest;
import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;

public class UpdateMetricHandler
  extends ExternalFacingRequestHandler<UpdateMetricRequestInternal, UpdateMetricResponse> {

  private static final UpdateMetricResponse RESPONSE = new UpdateMetricResponse();
  private final Provider<StoreManager> store;
  private final UpdateRetryer retry;

  @Inject
  public UpdateMetricHandler(Provider<StoreManager> manager,
    UpdateRetryer retryer,
    @Named(SchemaStoreModule.SCHEMA_UPDATE_RETRIES) int retries) {
    this.store = manager;
    this.retry = retryer;
    this.retry.setRetries(retries);
  }


  @Override
  public UpdateMetricResponse handle(UpdateMetricRequestInternal irequest, Context context)
    throws Exception {
    validateRequest(context, irequest);

    UpdateMetricRequest request = irequest.getBody();
    validateMetricRequest(context, request);

    boolean valid = validArray(request.getAliases());
    valid = valid || validArray(request.getTimestampPatterns());
    valid = valid || request.getNewDisplayName() != null;
    if (!valid) {
      return RESPONSE;
    }

    return retry.run(() -> {
      StoreManager manager = store.get();
      StoreManager.OrganizationBuilder builder = manager.updateOrg(irequest.getOrgId());
      StoreManager.MetricBuilder metric = builder.updateMetric(request.getMetricName());
      metric.addAliases(request.getAliases());
      metric.setDisplayName(request.getNewDisplayName());
      metric.withTimestampFormat(request.getTimestampPatterns());
      metric.build().commit();
      return RESPONSE;
    });
  }

  private boolean validArray(String[] fields) {
    return fields != null && fields.length > 0;
  }
}
