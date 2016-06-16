package io.fineo.schema.store;

import io.fineo.internal.customer.Metadata;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.exception.SchemaNotFoundException;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Actually do the management of getting things from the {@link SchemaStore} for you and packaging
 * it in easy to use ways.
 */
public class StoreClerk {

  private final SchemaStore store;
  private final String orgId;
  private final Metadata metadata;

  public StoreClerk(SchemaStore store, String orgId) {
    this.store = store;
    this.orgId = orgId;
    this.metadata = store.getOrgMetadata(orgId);
  }

  public List<String> getUserVisibleMetricNames() {
    return getUserVisibleNames(metadata);
  }

  public List<Metric> getMetrics() {
    return collectElementsForFields(metadata, (metricCname, metricUserName, aliases) -> {
      io.fineo.internal.customer.Metric metric = store.getMetricMetadata(orgId, metricCname);
      return new Metric(metricUserName, metric, orgId);
    });
  }

  /**
   * This just get the metric based on the alias name of the metric. We don't enforce that its
   * the user 'visible' name and return that metric as the name we specify.
   *
   * @param metricAliasName an alias of the metric name
   * @return helper to access fields of the metric
   */
  public Metric getMetricForUserNameOrAlias(String metricAliasName) throws SchemaNotFoundException {
    io.fineo.internal.customer.Metric metric =
      store.getMetricMetadataFromAlias(metadata, metricAliasName);
    SchemaUtils.checkFound(metric, metricAliasName, "metric");
    return new Metric(metricAliasName, metric, orgId);
  }

  public static class Metric {

    private final String userName;
    private final io.fineo.internal.customer.Metric metric;
    private Schema schema;
    private Map<String, String> reverseAliases;
    private final String orgId;

    public Metric(String userName, io.fineo.internal.customer.Metric metric, String orgId) {
      this.orgId = orgId;
      this.userName = userName;
      this.metric = metric;
    }

    public List<Field> getUserVisibleFields() {
      if (schema == null) {
        this.schema = new Schema.Parser().parse(metric.getMetricSchema());
      }
      return collectElementsForFields(metric.getMetadata(), (cname, userName, aliases) -> {
        Schema.Field field = schema.getField(cname);
        Schema.Type type = field.schema().getField("value").schema().getType();
        return new Field(userName, type, aliases);
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
      return getUserNameFromAliases(
        this.metric.getMetadata().getCanonicalNamesToAliases().get(fieldCname));
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

    public io.fineo.internal.customer.Metric getUnderlyingMetric() {
      return this.metric;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof Metric))
        return false;

      Metric metric1 = (Metric) o;

      return metric != null ? metric.equals(metric1.metric) : metric1.metric == null;
    }

    @Override
    public int hashCode() {
      return metric != null ? metric.hashCode() : 0;
    }
  }

  public static class Field {
    private final String name;
    private final Schema.Type type;
    private final List<String> aliases;

    public Field(String name, Schema.Type type, List<String> aliases) {
      this.name = name;
      this.type = type;
      this.aliases = aliases;
    }

    public String getName() {
      return name;
    }

    public Schema.Type getType() {
      return type;
    }

    public List<String> getAliases() {
      return aliases;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof Field))
        return false;

      Field field = (Field) o;

      if (!getName().equals(field.getName()))
        return false;
      if (getType() != field.getType())
        return false;
      return getAliases().equals(field.getAliases());

    }

    @Override
    public int hashCode() {
      int result = getName().hashCode();
      result = 31 * result + getType().hashCode();
      result = 31 * result + getAliases().hashCode();
      return result;
    }
  }

  private static String getUserVisibleName(Metadata meta, String cname) {
    return getUserNameFromAliases(meta.getCanonicalNamesToAliases().get(cname));
  }

  private static String getUserNameFromAliases(List<String> aliases) {
    return aliases != null && aliases.size() > 0 ? aliases.get(0) : null;
  }

  private static <T> List<T> collectElementsForFields(Metadata meta,
    FieldInstanceVisitor<T> func) {
    return meta.getCanonicalNamesToAliases().keySet()
               .stream()
               .map(cname -> {
                 String userName = getUserVisibleName(meta, cname);
                 if (userName == null) {
                   return null;
                 }
                 List<String> aliases = meta.getCanonicalNamesToAliases().get(cname);
                 List<String> remaining = new ArrayList<>(aliases);
                 remaining.remove(userName);
                 return func.call(cname, userName, remaining);
               })
               .filter(name -> name != null)
               .collect(toList());
  }

  private static List<String> getUserVisibleNames(Metadata meta) {
    return collectElementsForFields(meta, (cname, username, e) -> username);
  }

  public interface FieldInstanceVisitor<R> extends TriFunction<String, String, List<String>, R> {
  }

  @FunctionalInterface
  public interface TriFunction<F1, F2, F3, R> {
    public R call(F1 field1, F2 field2, F3 field3);
  }
}