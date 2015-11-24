package io.fineo.schema.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
@DynamoDBTable(tableName="ForgotToOverrideTheTableName")
public class SubjectSchema {

  private String subject;
  private String rangeKey;
  private Integer version;
  private Map<String, String> configs;
  private List<String> schemas;

  @DynamoDBHashKey(attributeName = DynamoDBRepository.PARTITION_KEY)
  public String getSubject() {
    return subject;
  }
  public SubjectSchema setSubject(String subject) {
    this.subject = subject;
    return this;
  }

  @DynamoDBRangeKey(attributeName = DynamoDBRepository.SORT_KEY)
  public String getRangeKey() {
    return rangeKey;
  }
  public SubjectSchema setRangeKey(String sortKey) {
    this.rangeKey = sortKey;
    return this;
  }

  @DynamoDBVersionAttribute(attributeName = DynamoDBRepository.VERSION_COLUMN)
  public Integer getVersion() {
    return version;
  }
  public SubjectSchema setVersion(Integer version) {
    this.version = version;
    return this;
  }

  @DynamoDBAttribute(attributeName = DynamoDBRepository.CONFIG_COLUMN)
  public Map<String, String> getConfigs() {
    return configs;
  }
  public SubjectSchema setConfigs(Map<String, String> configs) {
    this.configs = configs;
    return this;
  }

  @DynamoDBAttribute(attributeName = DynamoDBRepository.SCHEMAS_COLUMN)
  public List<String> getSchemas(){
    if(this.schemas == null){
      this.schemas = new ArrayList<>();
    }
    return this.schemas;
  }
  public SubjectSchema setSchemas(List<String> schemas) {
    this.schemas = schemas;
    return this;
  }
}
