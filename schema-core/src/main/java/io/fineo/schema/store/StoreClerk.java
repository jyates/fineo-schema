package io.fineo.schema.store;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.fineo.internal.customer.FieldMetadata;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.internal.customer.OrgMetricMetadata;
import io.fineo.schema.exception.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.schema.store.AvroSchemaProperties.TIMESTAMP_KEY;
import static java.util.stream.Collectors.toList;

/**
 * Actually do the management of getting things from the {@link SchemaStore} for you and packaging
 * it in easy to use ways.
 */
public class StoreClerk {

  private static final Logger LOG = LoggerFactory.getLogger(StoreClerk.class);
  private final SchemaStore store;
  private final String orgId;
  private final OrgMetadata metadata;

  public StoreClerk(SchemaStore store, String orgId) {
    this.store = store;
    this.orgId = orgId;
    this.metadata = Preconditions
      .checkNotNull(store.getOrgMetadata(orgId), "No schema stored for org: %s", orgId);
    LOG.debug("Got org metadata: {}", metadata);
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
    LOG.debug("In metadata:\n {},looking for metrics...", metadata);
    return collectElementsForFields(metadata, (metricCname, metricUserName, aliases) -> {
      io.fineo.internal.customer.Metric metric = store.getMetricMetadata(orgId, metricCname);
      LOG.debug("\tGot metric:\n {}", metric);
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
      LOG.debug("In metadata:\n {}, \nGot metric:\n {}", metadata, metric);
      return new Metric(metricUserName, metric, orgId, metricAliases);
    }).stream().findFirst().orElse(null);
    SchemaUtils.checkFound(foundMetric, metricAliasName, "metric");
    return foundMetric;
  }

  public Metric getMetricForCanonicalName(String metricId) {
    io.fineo.internal.customer.Metric metric = store.getMetricMetadata(orgId, metricId);
    OrgMetricMetadata metadata = this.metadata.getMetrics().get(metricId);
    LOG.debug("In metadata:\n {}, \nGot metric:\n {}", metadata, metric);
    return new Metric(metadata.getDisplayName(), metric, orgId, metadata.getAliasValues());
  }

  public Tenant getTenat() {
    return new Tenant(metadata);
  }

  public static class Tenant {
    private final OrgMetadata metadata;

    public Tenant(OrgMetadata metadata) {
      this.metadata = metadata;
    }

    public List<String> getTimestampPatterns() {
      List<String> patterns = metadata.getTimestampFormats();
      if (patterns == null) {
        patterns = Collections.emptyList();
      }
      return patterns;
    }

    public List<String> getMetricKeyAliases() {
      List<String> aliases = metadata.getMetricKeys();
      return aliases == null ? Collections.emptyList() : aliases;
    }
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
          Field field = buildField(cname, userName, aliases);
          return field.isInternalField() ? null : field;
        });
    }

    public Field getTimestampField() {
      // the avro way stores the type, but not the aliases
      Schema.Field field = getSchema().getField(AvroSchemaProperties.BASE_FIELDS_KEY);
      field = field.schema().getField(TIMESTAMP_KEY);

      // the canonical way stores the aliases, but not the type
      Field iField = this.getFieldForCanonicalName(AvroSchemaProperties.TIMESTAMP_KEY);
      List<String> aliases = newArrayList(iField.getAliases());
      aliases.remove(TIMESTAMP_KEY);
      return new Field(TIMESTAMP_KEY, field.schema().getType(), aliases, TIMESTAMP_KEY);
    }

    private Field buildField(String cname, String userName, List<String> aliases) {
      Schema.Field field = getSchema().getField(cname);
      if (field == null) {
        return new Field(userName, aliases, cname);
      }
      Schema.Type type = field.schema().getField("value").schema().getType();
      return new Field(userName, type, aliases, cname);
    }

    private Schema getSchema() {
      if (schema == null) {
        this.schema = new Schema.Parser().parse(metric.getMetricSchema());
      }
      return this.schema;
    }

    public List<FieldMetadata> getHiddenFields() {
      return metric.getMetadata().getFields().entrySet().stream()
                   .filter(entry -> entry.getValue().getHiddenTime() > 0)
                   .map(entry -> entry.getValue())
                   .collect(Collectors.toList());
    }

    public List<String> getCanonicalFieldNames() {
      return this.metric.getMetadata().getFields().entrySet()
                        .stream()
                        // hide internal fields
                        .filter(entry -> !entry.getValue().getInternalField())
                        .map(entry -> entry.getKey())
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

    public List<String> getTimestampPatterns() {
      List<String> patterns = this.metric.getMetadata().getTimestampFormats();
      if (patterns == null) {
        patterns = Collections.emptyList();
      }
      return patterns;
    }

    public Field getFieldForCanonicalName(String name) {
      FieldMetadata metadata = this.metric.getMetadata().getFields().get(name);
      if (metadata == null) {
        return null;
      }
      return buildField(name, metadata.getDisplayName(), metadata.getFieldAliases());
    }
  }

  public static class Field {
    private final String name;
    private final Schema.Type type;
    private final List<String> aliases;
    private final String cname;
    private final boolean internalField;

    public Field(String name, List<String> aliases, String cname) {
      this(name, null, aliases, cname, true);
    }

    public Field(String name, Schema.Type type, List<String> aliases, String cname) {
      this(name, type, aliases, cname, false);
    }

    private Field(String name, Schema.Type type, List<String> aliases, String cname,
      boolean internalField) {
      this.name = name;
      this.type = type;
      this.aliases = aliases;
      this.cname = cname;
      this.internalField = internalField;
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

    public boolean isInternalField() {
      return this.internalField;
    }
  }

  public OrgMetadata getOrgMetadataForTesting() {
    return this.metadata;
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
    R call(F1 field1, F2 field2, F3 field3);
  }
}
