package io.fineo.lambda.handle.schema.field.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.client.model.schema.field.UpdateFieldRequest;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.schema.store.StoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateFieldRequest;
import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;
import static io.fineo.lambda.handle.schema.inject.SchemaStoreModule.SCHEMA_UPDATE_RETRIES;

/**
 * A lambda handler that handles Kinesis events
 */
public class UpdateFieldHandler extends
                                ExternalFacingRequestHandler<UpdateFieldRequestInternal,
                                  UpdateFieldResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateFieldHandler.class);
  private static final UpdateFieldResponse RESPONSE = new UpdateFieldResponse();
  private final Provider<StoreManager> store;
  private final UpdateRetryer retry;

  @Inject
  public UpdateFieldHandler(Provider<StoreManager> store, UpdateRetryer retry,
    @Named(SCHEMA_UPDATE_RETRIES) int retries) {
    this.store = store;
    this.retry = retry;
    retry.setRetries(retries);
  }

  @Override
  protected UpdateFieldResponse handle(UpdateFieldRequestInternal irequest, Context context)
    throws Exception {
    validateRequest(context, irequest);

    UpdateFieldRequest request = irequest.getBody();
    validateFieldRequest(context, request);

    String[] aliases = request.getAliases();
    if (aliases == null || aliases.length == 0) {
      return RESPONSE;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("[{} - {}] Updating field: {} with aliases: '{}'", irequest.getOrgId(),
        request.getMetricName(), request.getFieldName(), request.getAliases());
    }
    return retry.run(() -> {
      StoreManager manager = store.get();
      manager.updateOrg(irequest.getOrgId()).updateMetric(request.getMetricName())
             .addFieldAlias(request.getFieldName(), aliases).build().commit();
      return RESPONSE;
    }, context);

  }
}
