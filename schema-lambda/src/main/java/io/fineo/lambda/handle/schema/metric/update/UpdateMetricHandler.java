package io.fineo.lambda.handle.schema.metric.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateMetricRequest;

public class UpdateMetricHandler
  extends ExternalFacingRequestHandler<UpdateMetricRequest, UpdateMetricResponse> {

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
  public UpdateMetricResponse handle(UpdateMetricRequest input, Context context) throws Exception {
    validateMetricRequest(context, input);
    boolean valid = validArray(input.getAliases());
    valid = valid || validArray(input.getTimestampPatterns());
    valid = valid || input.getNewDisplayName() != null;
    if (!valid) {
      return RESPONSE;
    }

    return retry.run(() -> {
      StoreManager manager = store.get();
      StoreManager.OrganizationBuilder builder = manager.updateOrg(input.getOrgId());
      StoreManager.MetricBuilder metric = builder.updateMetric(input.getMetricName());
      metric.addAliases(input.getAliases());
      metric.setDisplayName(input.getNewDisplayName());
      metric.withTimestampFormat(input.getTimestampPatterns());
      metric.build().commit();
      return RESPONSE;
    });
  }

  private boolean validArray(String[] fields) {
    return fields != null && fields.length > 0;
  }
}
