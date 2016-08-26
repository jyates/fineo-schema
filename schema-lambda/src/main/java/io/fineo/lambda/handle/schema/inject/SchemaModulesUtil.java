package io.fineo.lambda.handle.schema.inject;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Module;
import io.fineo.lambda.configure.dynamo.DynamoModule;
import io.fineo.lambda.configure.dynamo.DynamoRegionConfigurator;
import io.fineo.lambda.handle.schema.FieldRequest;
import io.fineo.lambda.handle.schema.MetricRequest;
import io.fineo.lambda.handle.schema.OrgRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.fineo.lambda.handle.LambdaBaseWrapper.addBasicProperties;

public class SchemaModulesUtil {
  private SchemaModulesUtil() {
  }

  @VisibleForTesting
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
    checkNotNull(context, org.getOrgId(), "Must specify an orgId!");
  }

  public static void validateMetricRequest(Context context, MetricRequest metric)
    throws JsonProcessingException {
    validateRequest(context, metric);
    checkNotNull(context, metric.getMetricName(), "Must specify a metric name!");
  }

  public static void validateFieldRequest(Context context, FieldRequest field)
    throws JsonProcessingException {
    validateMetricRequest(context, field);
    checkNotNull(context, field.getFieldName(), "Must specify a field!");
  }

  public static void checkNotNull(Context context, String field, String message)
    throws JsonProcessingException {
    if(field == null){
      throw40X(context, 0, message);
    }
  }

  public static void throw40X(Context context, int code, String message)
    throws JsonProcessingException {
    String type = null;
    switch (code) {
      case 0:
        type = "Bad Request";
        break;
      case 3:
        type = "Forbidden";
        break;
      case 4:
        type = "Not Found";
        break;
    }
    throwError(context, 400 + code, type, message);
  }

  public static void throwError(Context context, int code, String type,
    String message) throws JsonProcessingException {
    Map<String, Object> errorPayload = new HashMap();
    errorPayload.put("errorType", type);
    errorPayload.put("httpStatus", code);
    errorPayload.put("requestId", context.getAwsRequestId());
    errorPayload.put("message", message);
    throw new RuntimeException(new ObjectMapper().writeValueAsString(errorPayload));
  }
}
