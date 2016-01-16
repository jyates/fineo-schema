package io.fineo.schema.aws.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import io.fineo.schema.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.schemarepo.AbstractBackendRepository;
import org.schemarepo.Repository;
import org.schemarepo.RepositoryUtil;
import org.schemarepo.SchemaEntry;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;
import org.schemarepo.SubjectConfig;
import org.schemarepo.ValidatorFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link Repository} that stores data in DynamoDB
 * <p>
 * There are two row types: schema rows & dynamic rows. The schema rows operate like an RDBMS
 * stored row and should update atomically. They dynamic rows have a number of columns and a list
 * of their potential types (so we can do recommendation later)
 * </p>
 * <p>
 * Where the <tt>id</tt> is the String id of the subject and id-sort is either the id or
 * <tt>[id]_ext</tt>.Thus we can group together extension columns for the same id with the
 * original id.
 * </p>
 * <p>
 * Schema Row Attributes:
 * <ol>
 * <li>{@value #CONFIG_COLUMN} - stores the configuration for the schema as a string</li>
 * <li></li>
 * </ol>
 * </p>
 */
//TODO support client-side encryption http://java.awsblog
// .com/post/TxI32GE4IG2SNS/Client-side-Encryption-for-Amazon-DynamoDB
public class DynamoDBRepository extends AbstractBackendRepository {

  private static final Log LOG = LogFactory.getLog(DynamoDBRepository.class);
  public static final String PARTITION_KEY = "id";
  public static final String SORT_KEY = "id_s";
  public static final String CONFIG_COLUMN = "configs";
  public static final String VERSION_COLUMN = "version";
  public static final String SCHEMAS_COLUMN = "schemas";

  private final DynamoDB dynamo;
  private final Table table;
  private final AmazonDynamoDB client;
  private final DynamoDBMapper mapper;

  public DynamoDBRepository(ValidatorFactory validators, AmazonDynamoDB dynamoDB,
    CreateTableRequest schemaTable) {
    super(validators);
    this.client = dynamoDB;
    this.dynamo = new DynamoDB(dynamoDB);
    this.table = getTable(schemaTable);
    DynamoDBMapperConfig.Builder b = new DynamoDBMapperConfig.Builder();
    b.setTableNameOverride(new DynamoDBMapperConfig.TableNameOverride(table.getTableName()));
    b.setConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT);
    b.setSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE);
    this.mapper = new DynamoDBMapper(client, b.build());
  }

  public static DynamoDBRepository createDynamoForTesting(AmazonDynamoDB dynamodb,
    String schemaTable, ValidatorFactory validators) {
    CreateTableRequest create = getBaseTableCreate(schemaTable);
    create = create.withProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(10L)
      .withWriteCapacityUnits(5L));
    return new DynamoDBRepository(validators, dynamodb, create);
  }

  /**
   * @param table name of the schema table
   * @return the basics of a request to create the schema table. Missing throughout information
   * to be used to create the schema table
   */
  public static CreateTableRequest getBaseTableCreate(String table){
    Pair<List<KeySchemaElement>, List<AttributeDefinition>> schema = getSchema();
    CreateTableRequest create = new CreateTableRequest()
      .withTableName(table)
      .withKeySchema(schema.getKey())
      .withAttributeDefinitions(schema.getValue());
    return create;
  }

  private Table getTable(CreateTableRequest table) {
    Table t;
    try {
      // table probably exists, so check that first
      String name = table.getTableName();
      TableUtils.waitUntilExists(client, name, 1000, 100);
      t = dynamo.getTable(name);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (AmazonClientException e) {
      t = dynamo.createTable(table);
      LOG.info("Created Dynamo table: " + t);
    }

    return t;
  }

  @Override
  protected Subject getSubjectInstance(String subjectName) {
    waitForTable();
    return new DynamoSubject(subjectName);
  }

  @Override
  protected void registerSubjectInBackend(String subjectName, SubjectConfig config) {
    waitForTable();
    SubjectSchema schema = new SubjectSchema()
      .setSubject(subjectName)
      .setRangeKey(subjectName)
      .setConfigs(RepositoryUtil.safeConfig(config).asMap());
    try {
      mapper.save(schema);
    }catch(ConditionalCheckFailedException e){
      LOG.debug("Failed to register subject, it was already present!");
    }
  }


  @Override
  protected boolean checkSubjectExistsInBackend(final String subjectName) {
    GetItemSpec spec = new GetItemSpec();
    spec.withPrimaryKey(getPK(subjectName))
        .withConsistentRead(true)
        .withAttributesToGet(PARTITION_KEY);
    return table.getItem(spec) != null;

  }

  /**
   * Add a dynamic column to potentially be considered as part of the schema the next time we do
   * a schema change
   * <p>
   * No checking is done if this column is already part of the schema or not.
   * </p>
   *
   * @param column column to add
   * @param value  value of the column
   */
  public void addDynamicColumn(String subjectname, String column, String value) {
    waitForTable();
    UpdateItemSpec updateItemSpec = new UpdateItemSpec()
      .withPrimaryKey(getExtensionPK(subjectname))
      .withUpdateExpression("ADD #c :val1")
      .withNameMap(new NameMap()
        .with("#c", column))
      .withValueMap(new ValueMap()
        .withStringSet(":val1", value));
    table.updateItem(updateItemSpec);
  }

  /**
   * Companion to {@link #addDynamicColumn(String, String, String)}.
   *
   * @param subject
   * @return all the dynamic columns for the subject
   */
  public Map<String, Set<String>> getDynamicColumns(String subject) {
    waitForTable();
    GetItemSpec get = new GetItemSpec()
      .withPrimaryKey(getExtensionPK(subject))
      .withConsistentRead(true);
    Item item = table.getItem(get);
    Map<String, Set<String>> columns = new HashMap<>();
    for (Map.Entry<String, Object> column : item.attributes()) {
      // skip the PK columns
      if (column.getKey().equals(PARTITION_KEY) || column.getKey().equals(SORT_KEY)) {
        continue;
      }
      columns.put(column.getKey(), (Set<String>) column.getValue());
    }
    return columns;
  }

  private KeyAttribute[] getExtensionPK(String subject) {
    return getPK(subject, getExtensionName(subject));
  }

  private KeyAttribute[] getPK(String subject) {
    return getPK(subject, subject);
  }

  private KeyAttribute[] getPK(String partitionKey, String sortKey) {
    KeyAttribute[] attributes = new KeyAttribute[2];
    attributes[0] = new KeyAttribute(PARTITION_KEY, partitionKey);
    attributes[1] = new KeyAttribute(SORT_KEY, sortKey);
    return attributes;
  }

  private String getExtensionName(String subjectname) {
    return subjectname + "_ext";
  }

  private void waitForTable() {
    LOG.info("Waiting for " + table + " to become active");
    try {
      table.waitForActive();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
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
      .withAttributeName(PARTITION_KEY)
      .withKeyType(KeyType.HASH));
    attributes.add(new AttributeDefinition()
      .withAttributeName(PARTITION_KEY)
      .withAttributeType(ScalarAttributeType.S));

    // sort key
    schema.add(new KeySchemaElement()
      .withAttributeName(SORT_KEY)
      .withKeyType(KeyType.RANGE));
    attributes.add(new AttributeDefinition()
      .withAttributeName(SORT_KEY)
      .withAttributeType(ScalarAttributeType.S));

    return new Pair<>(schema, attributes);
  }

  /**
   * It is expected that the user will wrap this in a cache, so we don't do any explicit caching.
   */
  private class DynamoSubject extends Subject {
    private SubjectSchema subject;

    public DynamoSubject(String subjectName) {
      super(subjectName);
    }

    private void reload() {
      this.subject = mapper.load(SubjectSchema.class, getName(), getName());
    }

    @Override
    public boolean integralKeys() {
      return true;
    }

    @Override
    public SubjectConfig getConfig() {
      reload();
      return new SubjectConfig.Builder().set(this.subject.getConfigs()).build();
    }


    @Override
    public SchemaEntry register(String schema) throws SchemaValidationException {
      RepositoryUtil.validateSchemaOrSubject(schema);
      //get the latest state and check that it hasn't been added by someone else
      reload();
      List<String> schemas = this.subject.getSchemas();
      if (schemas.contains(schema)) {
        return asEntry(schemas.indexOf(schema), schema);
      }
      try {
        return write(schemas.size(), schema);
      } catch (ConditionalCheckFailedException e) {
        return register(schema);
      }
    }

    @Override
    public SchemaEntry registerIfLatest(String schema, SchemaEntry latest)
      throws SchemaValidationException {
      RepositoryUtil.validateSchemaOrSubject(schema);
      reload();
      if (latest == null)
        if (this.subject.getSchemas().size() == 0) {
          return write(0, schema);
        } else {
          return null;
        }

      if (latest.getId() == null) {
        return null;
      }

      int id;
      try {
        id = Integer.parseInt(latest.getId());
      } catch (NumberFormatException e) {
        return null;
      }
      if (!(id == subject.getSchemas().size() - 1) || !subject.getSchemas().get(id)
                                                              .equals(latest.getSchema())) {
        return null;
      }

      try {
        return write(subject.getSchemas().size(), schema);
      } catch (ConditionalCheckFailedException e) {
        // the schema is old, reload and try again
        return registerIfLatest(schema, latest);
      }
    }

    private SchemaEntry write(int index, String schema) {
      subject.getSchemas().add(schema);
      mapper.save(subject);
      return asEntry(index, schema);
    }

    private SchemaEntry asEntry(int index, String schema) {
      return new SchemaEntry(String.valueOf(index), schema);
    }

    @Override
    public SchemaEntry lookupBySchema(String schema) {
      reload();
      List<String> schemas = subject.getSchemas();
      if (!schemas.contains(schema)) {
        return null;
      }
      return asEntry(schemas, schemas.indexOf(schema));
    }

    @Override
    public SchemaEntry lookupById(String id) {
      reload();
      int index;
      try {
        index = Integer.parseInt(id);
      } catch (NumberFormatException e) {
        return null;
      }
      if (subject.getSchemas().size() - 1 < index) {
        return null;
      }
      return asEntry(subject.getSchemas(), index);
    }

    @Override
    public SchemaEntry latest() {
      reload();
      return asEntry(subject.getSchemas(), subject.getSchemas().size() - 1);
    }


    @Override
    public Iterable<SchemaEntry> allEntries() {
      reload();
      List<SchemaEntry> entries = new LinkedList<>();
      for (int i = 0; i < subject.getSchemas().size(); i++) {
        entries.add(0, asEntry(subject.getSchemas(), i));
      }

      return entries;
    }

    private SchemaEntry asEntry(Collection<String> schemas, int index) {
      if (index > schemas.size() - 1) {
        return null;
      }
      int i = 0;
      for (String schema : schemas) {
        if (i++ == index) {
          return new SchemaEntry(String.valueOf(index), schema);
        }
      }
      return null;
    }
  }
}