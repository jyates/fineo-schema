package io.fineo.lambda.handle.schema;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.aws.dynamodb.DynamoDBRepositoryTestUtils;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import org.schemarepo.ValidatorFactory;

import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaStoreModule.DYNAMO_SCHEMA_STORE_TABLE;

/**
 * Store module that ensures that the schema store table exists
 */
public class SchemaStoreModuleForTesting extends AbstractModule {

  public static void addBaseSchemaModules(List<Module> modules) {
    modules.add(new DynamoModule());
    modules.add(new SchemaStoreModuleForTesting());
  }

  @Override
  protected void configure() {
  }

  @Provides
  @Singleton
  public ValidatorFactory getSchemaValidation() {
    return ValidatorFactory.EMPTY;
  }

  @Provides
  @Inject
  @Singleton
  public SchemaStore getSchemaStore(ValidatorFactory factory, @Named(DYNAMO_SCHEMA_STORE_TABLE)
    String storeTable, AmazonDynamoDBAsyncClient dynamo) {
    DynamoDBRepository repo = DynamoDBRepositoryTestUtils.createDynamoForTesting(dynamo, storeTable,
      factory);
    return new SchemaStore(repo);
  }

  @Provides
  @Inject
  @Singleton
  public StoreManager getStoreManager(SchemaStore store) {
    return new StoreManager(store);
  }
}
