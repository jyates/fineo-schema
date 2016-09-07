package io.fineo.lambda.handle.schema.org;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;
import io.fineo.lambda.handle.schema.response.OrgResponse;

import java.io.IOException;
import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.getModules;

public class Org extends LambdaResponseWrapper<ExternalOrgRequest, OrgResponse, OrgHandler> {

  public Org() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public Org(List<Module> modules) {
    super(OrgHandler.class, modules);
  }

  @Override
  public OrgResponse handle(ExternalOrgRequest input, Context context) throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
