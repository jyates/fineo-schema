package io.fineo.lambda.handle.schema.field.update;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;

import java.io.IOException;
import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.getModules;

/**
 * Wrapper to instantiate the raw stage
 */
public class UpdateField extends
                         LambdaResponseWrapper<UpdateFieldRequest, UpdateFieldResponse, UpdateFieldHandler> {

  public UpdateField() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public UpdateField(List<Module> modules) {
    super(UpdateFieldHandler.class, modules);
  }

  @Override
  public UpdateFieldResponse handle(UpdateFieldRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
