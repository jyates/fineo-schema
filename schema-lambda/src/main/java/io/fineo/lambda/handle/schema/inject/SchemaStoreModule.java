package io.fineo.lambda.handle.schema.inject;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import org.schemarepo.Repository;
import org.schemarepo.ValidatorFactory;

import java.io.Serializable;

public class SchemaStoreModule extends AbstractModule implements Serializable {

  public static final String SCHEMA_UPDATE_RETRIES = "fineo.api.schema.retries";

  private final Class<? extends Provider<? extends Repository>> repoProvider;

  public SchemaStoreModule() {
    this(DynamoDBRepositoryProvider.class);
  }

  public SchemaStoreModule(Class<? extends Provider<? extends Repository>> repoProvider) {
    this.repoProvider = repoProvider;
  }

  @Override
  protected void configure() {
    bind(Repository.class).toProvider(repoProvider);
  }

  @Provides
  @Singleton
  public ValidatorFactory getSchemaValidation() {
    return ValidatorFactory.EMPTY;
  }

  @Inject
  @Provides
  @Singleton
  public SchemaStore getSchemaStore(Repository repo) {
    return new SchemaStore(repo);
  }

  @Inject
  @Provides
  @Singleton
  public StoreManager getStoreManager(SchemaStore store) {
    return new StoreManager(store);
  }
}
