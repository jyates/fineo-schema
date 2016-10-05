package io.fineo.schema.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import javafx.util.Pair;
import org.schemarepo.ValidatorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class DynamoDBRepositoryTestUtils {

  private DynamoDBRepositoryTestUtils(){
  }

  public static DynamoDBRepository createDynamoForTesting(AmazonDynamoDB dynamodb,
    String schemaTable, ValidatorFactory validators) {
    try {
      dynamodb.describeTable(schemaTable);
    } catch (ResourceNotFoundException e) {
      CreateTableRequest create = getBaseTableCreate(schemaTable);
      create = create.withProvisionedThroughput(new ProvisionedThroughput()
        .withReadCapacityUnits(10L)
        .withWriteCapacityUnits(5L));
      dynamodb.createTable(create);
    }
    return new DynamoDBRepository(validators, dynamodb, schemaTable);
  }

  /**
   * @param table name of the schema table
   * @return the basics of a request to create the schema table. Missing throughout information
   * to be used to create the schema table
   */
  public static CreateTableRequest getBaseTableCreate(String table) {
    Pair<List<KeySchemaElement>, List<AttributeDefinition>> schema = getSchema();
    CreateTableRequest create = new CreateTableRequest()
      .withTableName(table)
      .withKeySchema(schema.getKey())
      .withAttributeDefinitions(schema.getValue());
    return create;
  }

  /**
   * Get the DynamoDB schema that we should use when creating the table
   *
   * @return
   */
  public static Pair<List<KeySchemaElement>, List<AttributeDefinition>> getSchema() {
    List<KeySchemaElement> schema = new ArrayList<>();
    ArrayList<AttributeDefinition> attributes = new ArrayList<>();
    // Partition key
    schema.add(new KeySchemaElement()
      .withAttributeName(DynamoDBRepository.PARTITION_KEY)
      .withKeyType(KeyType.HASH));
    attributes.add(new AttributeDefinition()
      .withAttributeName(DynamoDBRepository.PARTITION_KEY)
      .withAttributeType(ScalarAttributeType.S));

    // sort key
    schema.add(new KeySchemaElement()
      .withAttributeName(DynamoDBRepository.SORT_KEY)
      .withKeyType(KeyType.RANGE));
    attributes.add(new AttributeDefinition()
      .withAttributeName(DynamoDBRepository.SORT_KEY)
      .withAttributeType(ScalarAttributeType.S));

    return new Pair<>(schema, attributes);
  }
}
