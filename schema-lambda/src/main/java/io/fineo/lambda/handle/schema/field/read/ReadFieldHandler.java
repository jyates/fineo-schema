package io.fineo.lambda.handle.schema.field.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.fineo.lambda.handle.ThrowingRequestHandler;
import io.fineo.lambda.handle.schema.inject.SchemaModulesUtil;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;

import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.checkNotNull;
import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.validateFieldRequest;
import static java.lang.String.format;

/**
 * A lambda handler that handles Kinesis events
 */
public class ReadFieldHandler extends
                              ThrowingRequestHandler<ReadFieldRequest, ReadFieldResponse> {
  private final Provider<SchemaStore> store;

  @Inject
  public ReadFieldHandler(Provider<SchemaStore> store) {
    this.store = store;
  }

  @Override
  protected ReadFieldResponse handle(ReadFieldRequest request, Context context)
    throws Exception {
    validateFieldRequest(context, request);

    StoreClerk clerk = new StoreClerk(store.get(), request.getOrgId());
    String metricName = request.getMetricName();
    try {
      StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
      String cname = metric.getCanonicalNameFromUserFieldName(request.getFieldName());
      checkNotNull(context, cname,
        format("No field with name or alias '%s' found", request.getFieldName()));
      List<StoreClerk.Field> fields = metric.getUserVisibleFields();
      for (StoreClerk.Field field : fields) {
        if (field.getCname().equals(cname)) {
          return asResponse(field);
        }
      }
    } catch (SchemaNotFoundException e) {
      SchemaModulesUtil.throw40X(context, 4, format("Metric [%s] not found!", metricName));
    }
    SchemaModulesUtil.throwError(context, 500, "Internal Server Error",
      "Found a matching internal name, but when searching the field, couldn't find a matching "
      + "field!");
    throw new IllegalStateException("Should never make it here!");
  }

  public static ReadFieldResponse asResponse(StoreClerk.Field field) {
    ReadFieldResponse response = new ReadFieldResponse();
    response.setName(field.getName());
    response.setAliases(field.getAliases().toArray(new String[0]));
    response.setType(field.getType().name());
    return response;
  }
}
