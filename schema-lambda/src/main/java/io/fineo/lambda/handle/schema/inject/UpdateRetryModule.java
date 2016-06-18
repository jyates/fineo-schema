package io.fineo.lambda.handle.schema.inject;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.fineo.lambda.handle.schema.UpdateRetryer;

/**
 * Module to create an {@link io.fineo.lambda.handle.schema.UpdateRetryer}
 */
public class UpdateRetryModule extends AbstractModule {

  @Override
  protected void configure() {
  }

  @Provides
  public UpdateRetryer getRetries() {
    return new UpdateRetryer();
  }
}
