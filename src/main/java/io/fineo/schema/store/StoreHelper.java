package io.fineo.schema.store;

import io.fineo.internal.customer.Metadata;
import io.fineo.schema.Pair;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Set of wrapper utilities to make it easier to manage schemas
 */
public class StoreHelper {

  private final SchemaStore store;
  private final String orgId;
  private final Metadata metadata;

  public StoreHelper(SchemaStore store, String orgId) {
    this.store = store;
    this.orgId = orgId;
    this.metadata = store.getOrgMetadata(orgId);
  }

  public List<String> getUserVisibleMetricNames() {
    return getUserVisibleNames(metadata);
  }

  public List<Metric> getMetrics() {
    return collectFieldForUserName(metadata, (metricCname, metricUserName) -> {
      io.fineo.internal.customer.Metric metric = store.getMetricMetadata(orgId, metricCname);
      return new Metric(metricUserName, metric);
    });
  }

  /**
   * This just get the metric based on the alias name of the metric. We don't enforce that its
   * the user 'visible' name and return that metric as the name we specify.
   *
   * @param metricAliasName an alias of the metric name
   * @return helper to access fields of the metric
   */
  public Metric getMetricForUserFieldName(String metricAliasName) {
    io.fineo.internal.customer.Metric metric =
      store.getMetricMetadataFromAlias(metadata, metricAliasName);
    assert metric != null;
    return new Metric(metricAliasName, metric);
  }

  public class Metric {

    private final String userName;
    private final io.fineo.internal.customer.Metric metric;
    private Schema schema;
    private Map<String, String> reverseAliases;

    public Metric(String userName, io.fineo.internal.customer.Metric metric) {
      this.userName = userName;
      this.metric = metric;
    }

    public List<Pair<String, Schema.Type>> getUserVisibleFields() {
      if (schema == null) {
        this.schema = new Schema.Parser().parse(metric.getMetricSchema());
      }
      return collectFieldForUserName(metric.getMetadata(), (cname, userName) -> {
        Schema.Field field = schema.getField(cname);
        Schema.Type type = field.schema().getField("value").schema().getType();
        return new Pair<>(userName, type);
      });
    }

    public List<String> getCanonicalFieldNames() {
      return this.metric.getMetadata().getCanonicalNamesToAliases().keySet().stream().filter(
        cname -> !cname.equals(AvroSchemaEncoder.BASE_FIELDS_KEY)).collect(Collectors.toList());
    }

    public String getUserName() {
      return userName;
    }

    public String getUserFieldNameFromCanonicalName(String fieldCname) {
      return StoreHelper.this
        .getUserName(this.metric.getMetadata().getCanonicalNamesToAliases().get(fieldCname));
    }

    public String getCanonicalNameFromUserFieldName(String fieldName) {
      if (this.reverseAliases == null) {
        this.reverseAliases = AvroSchemaManager.getAliasRemap(metric);
      }
      return this.reverseAliases.get(fieldName);
    }

    public String getOrgId() {
      return orgId;
    }

    public String getMetricId() {
      return this.metric.getMetadata().getCanonicalName();
    }
  }

  private String getUserVisibleName(Metadata meta, String cname) {
    return getUserName(meta.getCanonicalNamesToAliases().get(cname));
  }

  private String getUserName(List<String> aliases) {
    return aliases != null && aliases.size() > 0 ? aliases.get(0) : null;
  }

  private <T> List<T> collectFieldForUserName(Metadata meta, BiFunction<String, String, T> func) {
    return meta.getCanonicalNamesToAliases().keySet()
               .stream()
               .map(cname -> {
                 String userName = getUserVisibleName(meta, cname);
                 if (userName == null) {
                   return null;
                 }
                 return func.apply(cname, userName);
               })
               .filter(name -> name != null)
               .collect(toList());
  }

  private List<String> getUserVisibleNames(Metadata meta) {
    return collectFieldForUserName(meta, (cname, username) -> username);
  }
}
