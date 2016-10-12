package io.fineo.lambda.handle.schema.field.read;

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
public class ReadField
  extends LambdaResponseWrapper<ReadFieldRequestInternal, ReadFieldResponse, ReadFieldHandler> {

  public ReadField() throws IOException {
    this(getModules(PropertiesLoaderUtil.load()));
  }

  public ReadField(List<Module> modules) {
    super(ReadFieldHandler.class, modules);
  }

  @Override
  public ReadFieldResponse handle(ReadFieldRequestInternal input, Context context)
    throws IOException {
    return getInstance().handleRequest(input, context);
  }
}
