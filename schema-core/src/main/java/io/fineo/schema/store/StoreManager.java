package io.fineo.schema.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.fineo.internal.customer.Metric;
import io.fineo.internal.customer.OrgMetadata;
import io.fineo.schema.FineoStopWords;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.avro.SchemaNameGenerator;
import io.fineo.schema.exception.SchemaExistsException;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.exception.SchemaTypeNotFoundException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Easily manage and modify schema for an organization or previous.
 * <p>
 * From the end user perspective, the main things we want to do are:
 * <ol>
 * <li>Update an Org
 * <ul><li>addMetric(name, ... aliases)</li></ul>
 * </li>
 * <li>Update a Metric
 * <ul><li>setDisplayName(name)</li>
 * <li>addAlias(... aliases)</li>
 * <li>addField(name, type, alias)</li>
 * </ul>
 * </li>
 * <li>Update a field
 * <ul><li>#addAlias(... aliases)</li></ul></li>
 * </ol>
 * This class should make it much easier to do that than the older {@link SchemaBuilder} since we
 * expose things through the user visible names, rather than managing the canonical names.
 * <p>
 * This class should be used in concert with the {@link StoreClerk} to access the changes made by
 * the managed
 * </p>
 * </p>
 */
public class StoreManager {

  private final FineoStopWords stop = new FineoStopWords();
  private final SchemaNameGenerator generator;
  private final SchemaStore store;

  public StoreManager(SchemaStore store) {
    this(SchemaNameGenerator.DEFAULT_INSTANCE, store);
  }

  @VisibleForTesting
  StoreManager(SchemaNameGenerator generator, SchemaStore store) {
    this.generator = generator;
    this.store = store;
  }

  public OrganizationBuilder updateOrg(String orgId) {
    OrgMetadata org = store.getOrgMetadata(orgId);
    Preconditions.checkArgument(org != null, "No information present for tenant: '%s'", orgId);
    SchemaBuilder builder = SchemaBuilder.createForTesting(generator);
    return new OrganizationBuilder(org, builder.updateOrg(org));
  }

  public OrganizationBuilder newOrg(String orgId) throws SchemaExistsException {
    OrgMetadata previous = store.getOrgMetadata(orgId);
    if (previous != null) {
      throw new SchemaExistsException("Schema for org: " + orgId + " already exists!");
    }
    SchemaBuilder builder = SchemaBuilder.createForTesting(generator);
    return new OrganizationBuilder(null, builder.newOrg(orgId));
  }

  public class OrganizationBuilder {
    private final OrgMetadata previous;
    private final SchemaBuilder.OrganizationBuilder orgBuilder;
    private Map<String, Metric> updatedMetrics = new HashMap<>();

    public OrganizationBuilder(OrgMetadata previous, SchemaBuilder.OrganizationBuilder builder) {
      this.previous = previous;
      this.orgBuilder = builder;
      stop.recordStart();
    }

    public MetricBuilder newMetric() {
      SchemaBuilder.MetricBuilder metricBuilder = orgBuilder.newMetric();
      return new MetricBuilder(null, this, metricBuilder);
    }

    public MetricBuilder updateMetric(String userName) throws SchemaNotFoundException {
      Metric metric = getMetric(userName);
      SchemaBuilder.MetricBuilder metricBuilder = orgBuilder.updateSchema(metric);
      return new MetricBuilder(metric, this, metricBuilder);
    }

    public OrganizationBuilder deleteMetric(String userName) {
      Metric metric;
      try {
        metric = getMetric(userName);
      } catch (SchemaNotFoundException e) {
        // done! metric doesn't exist
        return this;
      }
      orgBuilder.deleteMetric(metric);
      return this;
    }

    public OrganizationBuilder withTimestampFormat(String... formats) {
      if (formats == null) {
        return this;
      }

      this.orgBuilder.withTimestampFormat(formats);
      return this;
    }

    public OrganizationBuilder withMetricKeys(String... keys) {
      if (keys == null) {
        return this;
      }

      this.orgBuilder.setMetricKeys(keys);
      return this;
    }

    private Metric getMetric(String name) throws SchemaNotFoundException {
      Metric metric = store.getMetricMetadataFromAlias(previous, name);
      SchemaUtils.checkFound(metric, name, "metric");
      return metric;
    }

    private void addMetric(String cname, Metric previous) {
      this.updatedMetrics.put(cname, previous);
    }

    public void commit() throws IOException, OldSchemaException {
      stop.endRecord();
      SchemaBuilder.Organization org = this.orgBuilder.build();
      if (previous == null) {
        store.createNewOrganization(org);
        return;
      }
      store.updateOrg(org, updatedMetrics, previous);
    }
  }

  public class MetricBuilder {

    private final OrganizationBuilder parent;
    private final Metric previous;
    private final SchemaBuilder.MetricBuilder metricBuilder;

    public MetricBuilder(Metric metric, OrganizationBuilder parent,
      SchemaBuilder.MetricBuilder metricBuilder) {
      this.previous = metric;
      this.parent = parent;
      this.metricBuilder = metricBuilder;
    }

    public MetricBuilder addAliases(String... aliases) {
      if (aliases != null) {
        for (String alias : aliases) {
          // happens from the API translation that we can get zero-length aliases, so skip those
          if (alias.length() == 0) {
            continue;
          }
          this.metricBuilder.withName(alias);
          stop.withField(alias);
        }
      }
      return this;
    }

    public MetricBuilder setDisplayName(String name) {
      if (name == null) {
        return this;
      }
      stop.withField(name);
      this.metricBuilder.withDisplayName(name);
      // ensure its added as an alias
      this.metricBuilder.withName(name);
      return this;
    }

    public NewFieldBuilder newField() {
      return new NewFieldBuilder(this, this.metricBuilder);
    }

    public MetricBuilder addFieldAlias(String fieldName, String... aliases)
      throws SchemaNotFoundException {
      String canonical =
        SchemaUtils.getCanonicalName(this.previous.getMetadata(), fieldName);
      SchemaUtils.checkFound(canonical, fieldName, "field (or alias)");
      SchemaBuilder.FieldBuilder fb = this.metricBuilder.updateField(canonical);
      for (String alias : aliases) {
        if (alias.length() == 0) {
          continue;
        }
        stop.withField(alias);
        fb.withAlias(alias);
      }
      fb.asField();

      return this;
    }

    public OrganizationBuilder build() throws IOException {
      String cname = this.metricBuilder.getMetricMetadata().getMeta().getCanonicalName();
      this.metricBuilder.build();
      this.parent.addMetric(cname, previous);
      return this.parent;
    }

    public MetricBuilder deleteField(String userFieldName) {
      Map<String, String> aliasToCaname = AvroSchemaManager.getAliasRemap(this.previous);
      String cname = aliasToCaname.get(userFieldName);
      // field doesn't exist! We are done.
      if (cname == null) {
        return this;
      }

      this.metricBuilder.updateField(cname).hardDelete();
      return this;
    }

    public MetricBuilder withTimestampFormat(String... formats) {
      if (formats == null || formats.length == 0) {
        return this;
      }

      this.metricBuilder.withTimestampFormat(formats);
      return this;
    }
  }

  public class NewFieldBuilder {

    private final MetricBuilder parent;
    private final SchemaBuilder.MetricBuilder builder;
    private String name;
    private List<String> aliases = ImmutableList.of();
    private String type;

    public NewFieldBuilder(MetricBuilder parent, SchemaBuilder.MetricBuilder metric) {
      this.parent = parent;
      this.builder = metric;
    }

    public NewFieldBuilder withName(String name) {
      this.name = name;
      stop.withField(name);
      return this;
    }

    public NewFieldBuilder withAliases(List<String> aliases) {
      this.aliases = aliases;
      return this;
    }

    public NewFieldBuilder withType(String typeName) {
      this.type = typeName;
      return this;
    }

    public NewFieldBuilder withType(Type t) {
      this.type = t.name();
      return this;
    }

    public MetricBuilder build() throws SchemaTypeNotFoundException {
      Preconditions.checkNotNull(this.name, "No name specified when building field!");
      Function<String, SchemaBuilder.FieldBuilder> func = getBuilderFunctionForType(type);
      SchemaBuilder.FieldBuilder fielder = func.apply(this.name);
      for (String alias : this.aliases) {
        if (alias.length() == 0) {
          continue;
        }
        fielder.withAlias(alias);
        stop.withField(alias);
      }
      fielder.asField();
      return parent;
    }

    private Function<String, SchemaBuilder.FieldBuilder> getBuilderFunctionForType(String type)
      throws SchemaTypeNotFoundException {
      String name = type.toUpperCase();
      name = name.replace(" ", "_");
      try {
        Type t = Type.valueOf(name);
        return t.func.apply(this.builder);
      } catch (IllegalArgumentException e) {
        throw new SchemaTypeNotFoundException("Type: " + type + " not a supported type!");
      }
    }
  }

  public enum Type {
    STRING(string), VARCHAR(string),
    BOOLEAN(b -> b::withBoolean),
    INTEGER(integer), INT(integer), SMALLINT(integer),
    LONG(longs), BIGINT(longs),
    FLOAT(b -> b::withFloat),
    DOUBLE(doubles), DOUBLE_PRECISION(doubles),
    BINARY(binary), BYTES(binary);

    private Function<SchemaBuilder.MetricBuilder, Function<String, SchemaBuilder.FieldBuilder>>
      func;

    Type(Function<SchemaBuilder.MetricBuilder, Function<String, SchemaBuilder.FieldBuilder>> func) {
      this.func = func;
    }
  }

  private static final Function<SchemaBuilder.MetricBuilder, Function<String, SchemaBuilder
    .FieldBuilder>>
    string = b -> b::withString;

  private static final Function<SchemaBuilder.MetricBuilder, Function<String, SchemaBuilder
    .FieldBuilder>>
    integer = b -> b::withInt;

  private static final Function<SchemaBuilder.MetricBuilder, Function<String, SchemaBuilder
    .FieldBuilder>>
    longs = b -> b::withLong;

  private static final Function<SchemaBuilder.MetricBuilder, Function<String, SchemaBuilder
    .FieldBuilder>>
    binary = b -> b::withBytes;

  private static final Function<SchemaBuilder.MetricBuilder, Function<String, SchemaBuilder
    .FieldBuilder>>
    doubles = b -> b::withDouble;
}
