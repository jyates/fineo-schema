package io.fineo.schema.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.fineo.internal.customer.Metadata;
import io.fineo.internal.customer.Metric;
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
    Metadata org = store.getOrgMetadata(orgId);
    SchemaBuilder builder = SchemaBuilder.createForTesting(generator);
    return new OrganizationBuilder(org, builder.updateOrg(org));
  }

  public OrganizationBuilder newOrg(String orgId) throws SchemaExistsException {
    Metadata previous = store.getOrgMetadata(orgId);
    if (previous != null) {
      throw new SchemaExistsException("Schema for org: " + orgId + " already exists!");
    }
    SchemaBuilder builder = SchemaBuilder.createForTesting(generator);
    return new OrganizationBuilder(null, builder.newOrg(orgId));
  }

  public class OrganizationBuilder {
    private final Metadata previous;
    ;
    private final SchemaBuilder.OrganizationBuilder orgBuilder;
    private Map<String, Metric> updatedMetrics = new HashMap<>();

    public OrganizationBuilder(Metadata previous, SchemaBuilder.OrganizationBuilder builder) {
      this.previous = previous;
      this.orgBuilder = builder;
    }

    public MetricBuilder newMetric() {
      SchemaBuilder.MetricBuilder metricBuilder = orgBuilder.newSchema();
      return new MetricBuilder(null, this, orgBuilder, metricBuilder);
    }

    public MetricBuilder updateMetric(String userName) throws SchemaNotFoundException {
      Metric metric = store.getMetricMetadataFromAlias(previous, userName);
      SchemaUtils.checkFound(metric, userName, "metric");
      SchemaBuilder.MetricBuilder metricBuilder = orgBuilder.updateSchema(metric);
      return new MetricBuilder(metric, this, orgBuilder, metricBuilder);
    }

    private void addMetric(String cname, Metric previous) {
      this.updatedMetrics.put(cname, previous);
    }

    public void commit() throws IOException, OldSchemaException {
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
    private final SchemaBuilder.OrganizationBuilder builder;
    private final Metric previous;
    private final SchemaBuilder.MetricBuilder metricBuilder;

    public MetricBuilder(Metric metric, OrganizationBuilder organizationBuilder,
      SchemaBuilder.OrganizationBuilder orgBuilder, SchemaBuilder.MetricBuilder metricBuilder) {
      this.previous = metric;
      this.parent = organizationBuilder;
      this.builder = orgBuilder;
      this.metricBuilder = metricBuilder;
    }

    public MetricBuilder addAliases(String... aliases) {
      for (String alias : aliases) {
        this.metricBuilder.withName(alias);
      }
      return this;
    }

    public MetricBuilder setDisplayName(String name) {
      this.metricBuilder.withDisplayName(name);
      return this;
    }

    public NewFieldBuilder newField() {
      return new NewFieldBuilder(this, this.metricBuilder);
    }

    public MetricBuilder addFieldAlias(String fieldName, String... aliases)
      throws SchemaNotFoundException {
      String canonical = SchemaUtils.getCanonicalName(this.previous.getMetadata(), fieldName);
      SchemaUtils.checkFound(canonical, fieldName, "field (or alias)");
      SchemaBuilder.FieldBuilder fb = this.metricBuilder.updateField(canonical);
      for (String alias : aliases) {
        fb.withAlias(alias);
      }
      fb.asField();

      return this;
    }

    public OrganizationBuilder build() throws IOException {
      String cname = this.metricBuilder.getCanonicalName();
      this.metricBuilder.build();
      this.parent.addMetric(cname, previous);
      return this.parent;
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
      return this;
    }

    public NewFieldBuilder withAliases(List<String> aliases) {
      this.aliases = aliases;
      return this;
    }

    public NewFieldBuilder withType(String typeName) {
      this.type = typeName.toUpperCase();
      return this;
    }

    public MetricBuilder build() throws SchemaTypeNotFoundException {
      Function<String, SchemaBuilder.FieldBuilder> func = getBuilderFunctionForType(type);
      SchemaBuilder.FieldBuilder fielder = func.apply(this.name);
      for (String alias : this.aliases) {
        fielder.withAlias(alias);
      }
      fielder.asField();
      return parent;
    }

    private Function<String, SchemaBuilder.FieldBuilder> getBuilderFunctionForType(String type)
      throws SchemaTypeNotFoundException {
      switch (type) {
        case "STRING":
        case "VARCHAR":
          return this.builder::withString;
        case "BOOLEAN":
          return this.builder::withBoolean;
        case "INTEGER":
        case "INT":
        case "SMALLINT":
          return this.builder::withInt;
        case "LONG":
        case "BIGINT":
          return this.builder::withLong;
        case "FLOAT":
          return this.builder::withFloat;
        case "DOUBLE":
        case "DOUBLE PRECISION":
          return this.builder::withDouble;
        case "BINARY":
        case "BYTES":
          return this.builder::withBytes;
      }
      throw new SchemaTypeNotFoundException("Type: " + type + " not a supported type!");
    }
  }
}
