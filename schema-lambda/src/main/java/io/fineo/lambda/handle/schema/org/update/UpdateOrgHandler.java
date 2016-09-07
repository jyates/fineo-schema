package io.fineo.lambda.handle.schema.org.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import com.google.inject.Provider;
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
  public ThrowingSupplier<UpdateOrgResponse> handle(UpdateOrgRequest input, Context
    context) throws JsonProcessingException {
    validateRequest(context, input);
    boolean valid = validArray(input.getMetricTypeKeys());
    valid = valid || validArray(input.getTimestampPatterns());
    if (!valid) {
      return () -> RESPONSE;
    }
    return () -> {
      StoreManager manager = store.get();
      manager.updateOrg(input.getOrgId())
             .withMetricKeys(input.getMetricTypeKeys())
             .withTimestampFormat(input.getTimestampPatterns())
             .commit();
      return RESPONSE;
    };
  }
}
