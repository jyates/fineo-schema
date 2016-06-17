package io.fineo.lambda.handle.schema.inject;

import com.google.common.base.Preconditions;
import com.google.inject.Module;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.handle.schema.FieldRequest;
import io.fineo.lambda.handle.schema.MetricRequest;
import io.fineo.lambda.handle.schema.OrgRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.fineo.lambda.handle.LambdaBaseWrapper.addBasicProperties;

public class SchemaModulesUtil {
  private SchemaModulesUtil() {
  }

  public static void addSchemaModules(List<Module> modules) {
    modules.add(new DynamoModule());
    modules.add(new DynamoRegionConfigurator());
    modules.add(new SchemaStoreModule());
  }

  public static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    addSchemaModules(modules);
    return modules;
  }

  public static void validateRequest(OrgRequest org) {
    Preconditions.checkNotNull(org.getOrgId(), "Must specify an orgId!");
  }

  public static void validateMetricRequest(MetricRequest metric) {
    validateRequest(metric);
    Preconditions.checkNotNull(metric.getMetricUserName(), "Must specify a metric name!");
  }

  public static void validateFieldRequest(FieldRequest field) {
    validateMetricRequest(field);
    Preconditions.checkNotNull(field.getUserFieldName(), "Must specify a field!");
  }
}
