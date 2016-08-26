package io.fineo.lambda.handle.schema.create;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.ThrowingErrorHandlerForSchema;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.store.StoreManager;

import java.io.IOException;

import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.validateRequest;

/**
 * Lambda handle to do synchronous creation of an org in the schema store
 */
public class CreateOrgHandler extends
                              ThrowingErrorHandlerForSchema<CreateOrgRequest, CreateOrgResponse> {

  private static final CreateOrgResponse RESPONSE = new CreateOrgResponse();
  private final Provider<StoreManager> store;

  @Inject
  public CreateOrgHandler(Provider<StoreManager> store) {
    this.store = store;
  }

  @Override
  public CreateOrgResponse handle(CreateOrgRequest input, Context context)
    throws IOException, OldSchemaException {
    validateRequest(context, input);
    StoreManager manager = store.get();
    String orgId = input.getOrgId();
    StoreManager.OrganizationBuilder builder = manager.newOrg(orgId);
    builder.commit();
    return RESPONSE;
  }
}
