package io.fineo.schema.store;

import com.google.common.collect.ImmutableList;
import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
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
  private final OrgMetadata metadata;

  public StoreClerk(SchemaStore store, String orgId) {
    this.store = store;
    this.orgId = orgId;
    this.metadata = store.getOrgMetadata(orgId);
  }

  public AvroSchemaEncoderFactory getEncoderFactory() throws
    SchemaNotFoundException {
    OrgMetadata orgMetadata = store.getOrgMetadata(orgId);
    return new AvroSchemaEncoderFactory(new StoreClerk(store, orgId), orgMetadata);
  }

  public List<String> getUserVisibleMetricNames() {
    return getUserVisibleNames(metadata);
  }

  public List<Metric> getMetrics() {
    return collectElementsForFields(metadata, (metricCname, metricUserName, aliases) -> {
      io.fineo.internal.customer.Metric metric = store.getMetricMetadata(orgId, metricCname);
      return new Metric(metricUserName, metric, orgId, aliases);
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
    String expected = store.getMetricCNameFromAlias(metadata, metricAliasName);
    Metric foundMetric = collectElementsForFields(metadata, (metricCname, metricUserName,
      metricAliases) -> {
      if (expected != metricCname) {
        return null;
      }

      io.fineo.internal.customer.Metric metric = store.getMetricMetadata(orgId, metricCname);
      return new Metric(metricUserName, metric, orgId, metricAliases);
    }).stream().findFirst().orElse(null);
    SchemaUtils.checkFound(foundMetric, metricAliasName, "metric");
    return foundMetric;
  }

  public Metric getMetricForCanonicalName(String metricId) {
    io.fineo.internal.customer.Metric metric = store.getMetricMetadata(orgId, metricId);
    OrgMetricMetadata metadata = this.metadata.getMetrics().get(metricId);
    return new Metric(metadata.getDisplayName(), metric, orgId, metadata.getAliasValues());
  }

  public static class Metric {

    private final String userName;
    private final io.fineo.internal.customer.Metric metric;
    private final List<String> aliases;
    private Schema schema;
    private Map<String, String> reverseAliases;
    private final String orgId;

    /**
     * Advanced use only! Create a Metric, but only with an underlying schema metric. This means
     * that you can only access methods that read from the metric, rather than metadata about the
     * metric itself
     * <p>
     * Used in Readerator
     *
     * @param metric underlying metric
     * @return a 'stunted' Metric
     */
    public static Metric metricOnlyFunctions(io.fineo.internal.customer.Metric metric) {
      return new Metric(null, metric, null, null);
    }

    private Metric(String userName, io.fineo.internal.customer.Metric metric, String orgId,
      List<String> aliases) {
      this.orgId = orgId;
      this.userName = userName;
      this.metric = metric;
      this.aliases = aliases;
    }

    public List<Field> getUserVisibleFields() {
      if (schema == null) {
        this.schema = new Schema.Parser().parse(metric.getMetricSchema());
      }
      return collectElementsForFields(
        new MetadataWrapper<FieldMetadata>() {
          @Override
          public Map<String, FieldMetadata> getFields() {
            return metric.getMetadata().getFields();
          }

          @Override
          public String getDisplayName(FieldMetadata field) {
            return field.getDisplayName();
          }

          @Override
          public List<String> getAliases(FieldMetadata field) {
            return field.getFieldAliases();
          }
        },
        (cname, userName, aliases) -> {
          Schema.Field field = schema.getField(cname);
          // this field was deleted.
          if (field == null) {
            return null;
          }
          Schema.Type type = field.schema().getField("value").schema().getType();
          return new Field(userName, type, aliases, cname);
        });
    }

    public List<FieldMetadata> getHiddenFields() {
      return metric.getMetadata().getFields().entrySet().stream()
                   .filter(entry -> entry.getValue().getHiddenTime() > 0)
                   .map(entry -> entry.getValue())
                   .collect(Collectors.toList());
    }

    public List<String> getCanonicalFieldNames() {
      return this.metric.getMetadata().getFields().keySet()
                        .stream()
                        .filter(cname -> !cname.equals(AvroSchemaProperties.BASE_FIELDS_KEY))
                        .collect(Collectors.toList());
    }

    public String getUserName() {
      return userName;
    }

    public String getUserFieldNameFromCanonicalName(String fieldCname) {
      return getUserNameFromAliases(
        this.metric.getMetadata().getFields().get(fieldCname).getFieldAliases());
    }

    public String getCanonicalNameFromUserFieldName(String fieldName) {
      if (AvroSchemaProperties.IS_BASE_FIELD.test(fieldName)) {
        return fieldName;
      }
      if (this.reverseAliases == null) {
        this.reverseAliases = AvroSchemaManager.getAliasRemap(metric);
      }
      return this.reverseAliases.get(fieldName);
    }

    public String getOrgId() {
      return orgId;
    }

    public String getMetricId() {
      return this.metric.getMetadata().getMeta().getCanonicalName();
    }

    public List<String> getAliases() {
      return aliases;
    }

    // Used by Readerator
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
    private final String cname;

    public Field(String name, Schema.Type type, List<String> aliases, String cname) {
      this.name = name;
      this.type = type;
      this.aliases = aliases;
      this.cname = cname;
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

    public String getCname() {
      return cname;
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
      if (!getAliases().equals(field.getAliases()))
        return false;
      return getCname().equals(field.getCname());

    }

    @Override
    public int hashCode() {
      int result = getName().hashCode();
      result = 31 * result + getType().hashCode();
      result = 31 * result + getAliases().hashCode();
      result = 31 * result + getCname().hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "Field{" +
             "name='" + name + '\'' +
             ", type=" + type +
             ", aliases=" + aliases +
             ", cname='" + cname + '\'' +
             '}';
    }
  }

  private static String getUserNameFromAliases(List<String> aliases) {
    return aliases != null && aliases.size() > 0 ? aliases.get(0) : null;
  }

  private static <T> List<T> collectElementsForFields(OrgMetadata meta,
    FieldInstanceVisitor<T> func) {
    return collectElementsForFields(new MetadataWrapper<OrgMetricMetadata>() {
      @Override
      public Map<String, OrgMetricMetadata> getFields() {
        return meta.getMetrics();
      }

      @Override
      public String getDisplayName(OrgMetricMetadata field) {
        return field.getDisplayName();
      }

      @Override
      public List<String> getAliases(OrgMetricMetadata field) {
        return field.getAliasValues();
      }
    }, func);
  }


  private static <T, FIELD_TYPE> List<T> collectElementsForFields(MetadataWrapper<FIELD_TYPE>
    meta, FieldInstanceVisitor<T> func) {
    // new org, nothing created yet
    if (meta.getFields() == null) {
      return ImmutableList.of();
    }
    return meta.getFields().entrySet()
               .stream()
               .filter(entry -> !AvroSchemaProperties.BASE_FIELDS_KEY.equals(entry.getKey()))
               .map(entry -> {
                 String cname = entry.getKey();
                 FIELD_TYPE field = entry.getValue();
                 String userName = meta.getDisplayName(field);
                 if (userName == null) {
                   return null;
                 }
                 List<String> aliases = meta.getAliases(field);
                 List<String> remaining = new ArrayList<>(aliases);
                 remaining.remove(userName);
                 return func.call(cname, userName, remaining);
               })
               .filter(name -> name != null)
               .collect(toList());
  }

  private interface MetadataWrapper<T> {
    Map<String, T> getFields();

    String getDisplayName(T field);

    List<String> getAliases(T field);
  }

  private static List<String> getUserVisibleNames(OrgMetadata meta) {
    return collectElementsForFields(meta, (cname, username, e) -> username);
  }

  public interface FieldInstanceVisitor<R> extends TriFunction<String, String, List<String>, R> {
  }

  @FunctionalInterface
  public interface TriFunction<F1, F2, F3, R> {
    public R call(F1 field1, F2 field2, F3 field3);
  }
}
