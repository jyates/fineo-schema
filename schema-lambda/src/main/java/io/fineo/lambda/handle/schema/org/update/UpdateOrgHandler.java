package io.fineo.lambda.handle.schema.org.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.fineo.client.model.schema.SchemaManagementRequest;
import io.fineo.lambda.handle.schema.ThrowingSupplier;
import io.fineo.lambda.handle.schema.org.RunnableHandler;
import io.fineo.schema.store.StoreManager;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;

public class UpdateOrgHandler extends RunnableHandler<UpdateOrgRequest, UpdateOrgResponse> {

  private static final UpdateOrgResponse RESPONSE = new UpdateOrgResponse();
  private final Provider<StoreManager> store;

  @Inject
  public UpdateOrgHandler(Provider<StoreManager> manager) {
    this.store = manager;
  }

  @Override
  public ThrowingSupplier<UpdateOrgResponse> handle(UpdateOrgRequest irequest, Context
    context) throws JsonProcessingException {
    validateRequest(context, irequest);
    SchemaManagementRequest request = irequest.getBody();
    boolean valid = validArray(request.getMetricTypeKeys());
    valid = valid || validArray(request.getTimestampPatterns());
    if (!valid) {
      return () -> RESPONSE;
    }
    return () -> {
      StoreManager manager = store.get();
      manager.updateOrg(irequest.getOrgId())
             .withMetricKeys(request.getMetricTypeKeys())
             .withTimestampFormat(request.getTimestampPatterns())
             .commit();
      return RESPONSE;
    };
  }
}
