package io.fineo.lambda.handle.schema.create;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.fineo.lambda.handle.schema.field.UpdateFieldRequest;
import io.fineo.lambda.handle.schema.field.UpdateFieldResponse;

import io.fineo.lambda.handle.LambdaResponseWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Wrapper to instantiate the raw stage
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
  public UpdateFieldResponse handle(UpdateFieldRequest event) throws IOException {
    return getInstance().handle(event);
  }

  @VisibleForTesting
  public static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    // add more Guice modules here
    return modules;
  }
}
