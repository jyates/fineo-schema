package io.fineo.lambda.handle.schema.field;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Wrapper to instantiate the raw stage
 */
public class UpdateField extends LambdaResponseWrapper<UpdateFieldRequest, UpdateFieldResponse>{

  public UpdateField() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public UpdateField(List<Module> modules) {
    super(UpdateFieldHandler.class, modules);
  }

  @Override
  public UpdateFieldResponse handle(UpdateFieldRequest event, Context context) throws IOException {
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
