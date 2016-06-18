package io.fineo.lambda.handle.schema.field.delete;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;
import io.fineo.lambda.handle.schema.field.update.UpdateFieldHandler;
import io.fineo.lambda.handle.schema.field.update.UpdateFieldRequest;
import io.fineo.lambda.handle.schema.field.update.UpdateFieldResponse;

import java.io.IOException;
import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.getModules;

/**
 * Wrapper to handle requests to delete a field from a metric
 */
public class DeleteField extends
                         LambdaResponseWrapper<DeleteFieldRequest, DeleteFieldResponse, DeleteFieldHandler> {

  public DeleteField() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public DeleteField(List<Module> modules) {
    super(DeleteFieldHandler.class, modules);
  }

  @Override
  public DeleteFieldResponse handle(DeleteFieldRequest input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}