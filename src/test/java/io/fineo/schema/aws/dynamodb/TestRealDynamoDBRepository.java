package io.fineo.schema.aws.dynamodb;

import io.fineo.aws.AwsDependentTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.schemarepo.AbstractTestPersistentRepository;

import java.util.UUID;

/**
 *
 */
@Category(AwsDependentTests.class)
public class TestRealDynamoDBRepository extends AbstractTestPersistentRepository<DynamoDBRepository> {

  private static String testTableName = "schema-repository-test-"+UUID.randomUUID().toString();

  @BeforeClass
  public static void setupTestTable(){

  }

  @AfterClass
  public static void removeTestTable(){
    
  }

  @Override
  protected DynamoDBRepository createRepository() throws Exception {
    return null;
  }
}
