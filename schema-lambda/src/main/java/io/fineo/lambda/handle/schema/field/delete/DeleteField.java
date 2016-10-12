package io.fineo.lambda.handle.schema.field.delete;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Module;
import io.fineo.lambda.configure.util.PropertiesLoaderUtil;
import io.fineo.lambda.handle.LambdaResponseWrapper;

import java.io.IOException;
import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.getModules;

/**
 * Wrapper to handle requests to delete a field from a metric
 */
public class DeleteField extends
                         LambdaResponseWrapper<DeleteFieldRequestInternal, DeleteFieldResponse, DeleteFieldHandler> {

  public DeleteField() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public DeleteField(List<Module> modules) {
    super(DeleteFieldHandler.class, modules);
  }

  @Override
  public DeleteFieldResponse handle(DeleteFieldRequestInternal input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
