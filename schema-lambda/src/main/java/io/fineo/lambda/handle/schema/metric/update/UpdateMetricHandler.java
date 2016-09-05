package io.fineo.lambda.handle.schema.metric.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.schema.ThrowingErrorHandlerForSchema;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateMetricRequest;

public class UpdateMetricHandler
  extends ThrowingErrorHandlerForSchema<UpdateMetricRequest, UpdateMetricResponse> {

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
    boolean validAliases = validArray(input.getAliases());
    boolean validNewKeys = validArray(input.getNewKeys());
    boolean validRemoveKeys = validArray(input.getRemoveKeys());
    boolean validDisplay = input.getNewDisplayName() != null;
    if (!validAliases && !validNewKeys && !validRemoveKeys && !validDisplay) {
      return RESPONSE;
    }

    return retry.run(() -> {
      StoreManager manager = store.get();
      StoreManager.OrganizationBuilder builder = manager.updateOrg(input.getOrgId());
      StoreManager.MetricBuilder metric = builder.updateMetric(input.getMetricName());
      metric.addAliases(input.getAliases());
      metric.setDisplayName(input.getNewDisplayName());
      metric.addKeyAliases(input.getNewKeys());
      metric.removeKeyAliases(input.getRemoveKeys());
      metric.build().commit();
      return RESPONSE;
    });
  }

  private boolean validArray(String[] fields) {
    return fields != null && fields.length > 0;
  }
}
