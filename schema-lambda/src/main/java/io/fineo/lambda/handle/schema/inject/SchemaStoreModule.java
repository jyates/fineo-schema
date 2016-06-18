package io.fineo.lambda.handle.schema.inject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import org.schemarepo.ValidatorFactory;

import java.io.Serializable;
import java.util.List;

public class SchemaStoreModule extends AbstractModule implements Serializable {

  public static final String DYNAMO_SCHEMA_STORE_TABLE = "fineo.dynamo.schema.table";
  public static final String DYNAMO_READ_LIMIT = "fineo.dynamo.schema.limit.read";
  public static final String DYNAMO_WRITE_LIMIT = "fineo.dynamo.schema.limit.write";
  public static final String SCHEMA_UPDATE_RETRIES = "fineo.schema.retries";

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
  public SchemaStore getSchemaStore(ValidatorFactory factory, CreateTableRequest create,
    AmazonDynamoDBAsyncClient dynamo) {
    DynamoDBRepository repo =
      new DynamoDBRepository(factory, dynamo, create);
    return new SchemaStore(repo);
  }

  @Provides
  @Inject
  public CreateTableRequest getDynamoSchemaTable(
    @Named(DYNAMO_SCHEMA_STORE_TABLE) String storeTable,
    @Named(DYNAMO_READ_LIMIT) Long read, @Named(DYNAMO_WRITE_LIMIT) Long write) {
    CreateTableRequest create =
      DynamoDBRepository.getBaseTableCreate(storeTable);
    create.setProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(read)
      .withWriteCapacityUnits(write));
    return create;
  }

  @Provides
  @Inject
  @Singleton
  public StoreManager getStoreManager(SchemaStore store) {
    return new StoreManager(store);
  }
}
