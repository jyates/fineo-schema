package io.fineo.lambda.handle.schema.org;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.schema.ThrowingSupplier;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

/**
 * Execute requests
 */
public class RequestRunner {

  private final Provider<StoreManager> store;
  private final UpdateRetryer retry;

  @Inject
  public RequestRunner(Provider<StoreManager> manager, UpdateRetryer retryer,
    @Named(SchemaStoreModule.SCHEMA_UPDATE_RETRIES) int retries) {
    this.store = manager;
    this.retry = retryer;
    this.retry.setRetries(retries);
  }

  public <T> T run(ThrowingSupplier<T> supplier) throws Exception {
    return retry.run(supplier);
  }
}
