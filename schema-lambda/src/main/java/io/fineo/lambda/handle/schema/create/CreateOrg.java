package io.fineo.lambda.handle.schema.create;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;

import java.io.IOException;
import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.getModules;

/**
 * Wrapper to instantiate support the "create org" api endpoint
 */
public class CreateOrg extends
                       LambdaResponseWrapper<CreateOrgRequest, CreateOrgResponse, CreateOrgHandler>{

  public CreateOrg() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public CreateOrg(List<Module> modules) {
    super(CreateOrgHandler.class, modules);
  }

  @Override
  public CreateOrgResponse handle(CreateOrgRequest createOrgRequest, Context context)
    throws IOException {
    return getInstance().handleRequest(createOrgRequest, context);
  }
}
