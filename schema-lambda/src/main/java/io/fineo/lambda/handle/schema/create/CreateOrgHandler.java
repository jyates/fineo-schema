package io.fineo.lambda.handle.schema.create;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.fineo.lambda.handle.external.ExternalErrorsUtil;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.exception.SchemaExistsException;
import io.fineo.schema.store.StoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;

/**
 * Lambda handle to do synchronous creation of an org in the schema store
 */
public class CreateOrgHandler extends
                              ExternalFacingRequestHandler<CreateOrgRequest, CreateOrgResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateOrgHandler.class);
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
    LOG.debug("Attempting to create org: {}", orgId);
    try {
      StoreManager.OrganizationBuilder builder = manager.newOrg(orgId);
      builder.commit();
    } catch (SchemaExistsException e) {
      throw ExternalErrorsUtil.get40X(context, 0, e.getMessage());
    }
    return RESPONSE;
  }
}
