package io.fineo.lambda.handle.schema.inject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.schema.aws.dynamodb.DynamoDBRepository;
import org.schemarepo.Repository;
import org.schemarepo.ValidatorFactory;

public class DynamoDBRepositoryProvider implements Provider<Repository> {

  public static final String DYNAMO_SCHEMA_STORE_TABLE = "fineo.dynamo.schema-store";

  private final ValidatorFactory factory;
  private final String storeTableName;
  private final AmazonDynamoDBAsyncClient client;
  private DynamoDBRepository repo;

  @Inject
  public DynamoDBRepositoryProvider(ValidatorFactory factory,
    @Named(DYNAMO_SCHEMA_STORE_TABLE) String storeTableName,
    AmazonDynamoDBAsyncClient client) {
    this.factory = factory;
    this.storeTableName = storeTableName;
    this.client = client;
  }

  @Override
  public Repository get() {
    if(this.repo == null){
      this.repo = new DynamoDBRepository(factory, client, storeTableName);
    }
    return this.repo;
  }
}
