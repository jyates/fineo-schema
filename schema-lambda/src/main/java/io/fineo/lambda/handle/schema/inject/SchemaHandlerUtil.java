package io.fineo.lambda.handle.schema.inject;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.inject.Module;
import io.fineo.client.model.schema.field.FieldRequest;
import io.fineo.client.model.schema.metric.MetricRequest;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.handle.external.ExternalErrorsUtil;
import io.fineo.lambda.handle.schema.request.FieldRequestInternal;
import io.fineo.lambda.handle.schema.request.MetricRequestInternal;
import io.fineo.lambda.handle.schema.request.OrgRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.fineo.lambda.handle.LambdaBaseWrapper.addBasicProperties;

public class SchemaHandlerUtil {
  private SchemaHandlerUtil() {
  }

  public static void addBaseSchemaModules(List<Module> modules) {
    modules.add(new DynamoModule());
    modules.add(new SchemaStoreModule());
  }

  public static void addSchemaModules(List<Module> modules) {
    addBaseSchemaModules(modules);

    // production specific so we can connect to a 'real' instance
    modules.add(new DynamoRegionConfigurator());
  }

  public static List<Module> getModules(Properties props) {
    List<Module> modules = new ArrayList<>();
    addBasicProperties(modules, props);
    addSchemaModules(modules);
    return modules;
  }

  public static void validateRequest(Context context, OrgRequest org)
    throws JsonProcessingException {
    Preconditions.checkNotNull(org, "No message!");
    checkNotNull(context, org.getOrgId(), "Must specify an orgId!");
  }

  public static void validateMetricRequest(Context context, MetricRequestInternal metric)
    throws JsonProcessingException {
    validateRequest(context, metric);
    validateMetricRequest(context, metric.getBody());
  }

  public static void validateMetricRequest(Context context, MetricRequest request)
    throws JsonProcessingException {
    checkNotNull(context, request, "No request body!");
    checkNotNull(context, request.getMetricName(), "Must specify a metric name!");
  }

  public static void validateFieldRequest(Context context, FieldRequestInternal field)
    throws JsonProcessingException {
    validateMetricRequest(context, field);
    validateFieldRequest(context, field.getBody());
  }

  public static void validateFieldRequest(Context context, FieldRequest field)
    throws JsonProcessingException {
    validateMetricRequest(context, field);
    checkNotNull(context, field.getFieldName(), "Must specify a field!");
  }

  public static <T> void checkNotNull(Context context, T field, String message,
    Object... errorMessageArgs)
    throws JsonProcessingException {
    if (errorMessageArgs != null && errorMessageArgs.length > 0) {
      message = String.format(message, errorMessageArgs);
    }
    if (field == null || (field instanceof String && ((String) field).length() == 0)) {
      throw ExternalErrorsUtil.get40X(context, 0, message);
    }
  }
}
