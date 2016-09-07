package io.fineo.lambda.handle.schema.org.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.ThrowingSupplier;
import io.fineo.lambda.handle.schema.org.RunnableHandler;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;

public class ReadOrgHandler extends RunnableHandler<ReadOrgRequest, ReadOrgResponse> {

  private static final String[] EMTPY = new String[0];
  private final Provider<SchemaStore> store;

  @Inject
  public ReadOrgHandler(Provider<SchemaStore> store) {
    this.store = store;
  }

  @Override
  public ThrowingSupplier<ReadOrgResponse> handle(ReadOrgRequest input, Context
    context) throws JsonProcessingException {
    validateRequest(context, input);
    return () -> {
      StoreClerk clerk = new StoreClerk(store.get(), input.getOrgId());
      StoreClerk.Tenant tenant = clerk.getTenat();
      ReadOrgResponse response = new ReadOrgResponse();
      response.setMetricKeys(tenant.getMetricKeyAliases().toArray(EMTPY));
      response.setTimestampPatterns(tenant.getTimestampPatterns().toArray(EMTPY));
      return response;
    };
  }
}
