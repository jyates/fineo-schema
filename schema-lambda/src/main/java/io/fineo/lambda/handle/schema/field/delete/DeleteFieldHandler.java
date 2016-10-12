package io.fineo.lambda.handle.schema.field.delete;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.schema.store.StoreManager;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateMetricRequest;
import static io.fineo.lambda.handle.schema.inject.SchemaStoreModule.SCHEMA_UPDATE_RETRIES;

/**
 * A lambda handler that handles Kinesis events
 */
public class DeleteFieldHandler extends
                                ExternalFacingRequestHandler<DeleteFieldRequestInternal, DeleteFieldResponse> {

  private static final DeleteFieldResponse RESPONSE = new DeleteFieldResponse();
  private final Provider<StoreManager> store;
  private final UpdateRetryer retry;

  @Inject
  public DeleteFieldHandler(Provider<StoreManager> store, UpdateRetryer retry,
    @Named(SCHEMA_UPDATE_RETRIES) int retries) {
    this.store = store;
    this.retry = retry;
    retry.setRetries(retries);
  }

  @Override
  protected DeleteFieldResponse handle(DeleteFieldRequestInternal request, Context context)
    throws Exception {
    validateMetricRequest(context, request);

    return retry.run(() -> {
      StoreManager manager = store.get();
      manager.updateOrg(request.getOrgId()).updateMetric(request.getBody().getMetricName())
             .deleteField(request.getBody().getFieldName()).build().commit();
      return RESPONSE;
    }, context);

  }
}
