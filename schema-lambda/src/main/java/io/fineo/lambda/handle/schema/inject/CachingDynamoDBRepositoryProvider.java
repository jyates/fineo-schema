package io.fineo.lambda.handle.schema.inject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.schemarepo.CacheRepository;
import org.schemarepo.InMemoryCache;
import org.schemarepo.Repository;
import org.schemarepo.ValidatorFactory;

/**
 * Wrapper around the {@link DynamoDBRepositoryProvider} that provides a cached version of the
 * schema repository
 */
public class CachingDynamoDBRepositoryProvider extends DynamoDBRepositoryProvider {

  @Inject
  public CachingDynamoDBRepositoryProvider(ValidatorFactory factory,
    @Named(DYNAMO_SCHEMA_STORE_TABLE) String storeTableName,
    AmazonDynamoDBAsyncClient client) {
    super(factory, storeTableName, client);
  }

  @Override
  public Repository get() {
    Repository repository = super.get();
    return new CacheRepository(repository, new InMemoryCache());
  }
}
