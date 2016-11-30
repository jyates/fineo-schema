package io.fineo.lambda.handle.schema.org;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.fineo.lambda.handle.schema.ThrowingSupplier;

/**
 *
 */
public abstract class RunnableHandler<REQUEST, RESPONSE>{

  public abstract ThrowingSupplier<RESPONSE> handle(REQUEST request, Context context)
    throws JsonProcessingException;


  protected boolean validArray(String[] fields) {
    return fields != null;
  }

}
