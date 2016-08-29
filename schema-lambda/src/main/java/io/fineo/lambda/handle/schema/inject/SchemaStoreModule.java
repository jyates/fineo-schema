package io.fineo.lambda.handle.schema.inject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import org.schemarepo.ValidatorFactory;

import java.io.Serializable;

public class SchemaStoreModule extends AbstractModule implements Serializable {

  public static final String DYNAMO_SCHEMA_STORE_TABLE = "fineo.dynamo.schema-store";
  public static final String SCHEMA_UPDATE_RETRIES = "fineo.api.schema.retries";

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
    DynamoDBRepository repo =
      new DynamoDBRepository(factory, dynamo, storeTable);
    return new SchemaStore(repo);
  }

  @Provides
  @Inject
  @Singleton
  public StoreManager getStoreManager(SchemaStore store) {
    return new StoreManager(store);
  }
}
