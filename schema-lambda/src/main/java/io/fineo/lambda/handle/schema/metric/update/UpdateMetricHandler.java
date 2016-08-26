package io.fineo.lambda.handle.schema.metric.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.schema.ThrowingErrorHandlerForSchema;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.validateMetricRequest;

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
    boolean skipAliases = input.getAliases() == null || input.getAliases().length == 0;
    boolean skipDisplay = input.getNewDisplayName() == null;
    if (skipAliases && skipDisplay) {
      return RESPONSE;
    }

    return retry.run(() -> {
      StoreManager manager = store.get();
      StoreManager.MetricBuilder metric = manager.updateOrg(input.getOrgId())
                                                 .updateMetric(input.getMetricName());
      if (!skipAliases) {
        metric.addAliases(input.getAliases());
      }
      if (!skipDisplay) {
        metric.setDisplayName(input.getNewDisplayName());
      }
      metric.build().commit();
      return RESPONSE;
    });
  }
}
