package io.fineo.lambda.handle.schema.org;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.org.read.ReadOrgHandler;
import io.fineo.lambda.handle.schema.org.read.ReadOrgRequest;
import io.fineo.lambda.handle.schema.org.update.UpdateOrgHandler;
import io.fineo.lambda.handle.schema.org.update.UpdateOrgRequest;
import io.fineo.lambda.handle.schema.response.OrgResponse;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;

/**
 *
 */
public class OrgHandler extends ExternalFacingRequestHandler<ExternalOrgRequest, OrgResponse> {

  private final RequestRunner runner;
  private final UpdateOrgHandler patch;
  private final ReadOrgHandler get;

  @Inject
  public OrgHandler(RequestRunner runner, UpdateOrgHandler patch, ReadOrgHandler get) {
    this.runner = runner;
    this.patch = patch;
    this.get = get;
  }

  @Override
  protected OrgResponse handle(ExternalOrgRequest orgRequest, Context context) throws Exception {
    String type = Preconditions.checkNotNull(orgRequest.getType(),
      "No request type provided to internal microservice");
    validateRequest(context, orgRequest);
    switch (type.toUpperCase()) {
      case "GET":
        ReadOrgRequest request = new ReadOrgRequest();
        request.setOrgId(orgRequest.getOrgId());
        return get.handle(request, context).doGet();
      case "PATCH":
        UpdateOrgRequest patchRequest = new UpdateOrgRequest();
        patchRequest.setOrgId(orgRequest.getOrgId());
        patchRequest.setBody(orgRequest.getPatch());
        return runner.run(patch.handle(patchRequest, context), context);
      default:
        throw new IllegalArgumentException("Unsupported request type: " + orgRequest.getType());
    }
  }
}
