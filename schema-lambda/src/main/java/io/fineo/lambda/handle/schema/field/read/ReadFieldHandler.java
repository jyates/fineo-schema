package io.fineo.lambda.handle.schema.field.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.fineo.client.model.schema.field.FieldRequest;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;

import java.util.List;

import static io.fineo.lambda.handle.external.ExternalErrorsUtil.get40X;
import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateFieldRequest;
import static java.lang.String.format;

/**
 * A lambda handler that handles Kinesis events
 */
public class ReadFieldHandler extends
                              ExternalFacingRequestHandler<ReadFieldRequestInternal, ReadFieldResponse> {

  private final Provider<SchemaStore> store;

  private static final String[] EMPTY_STRING = new String[0];

  @Inject
  public ReadFieldHandler(Provider<SchemaStore> store) {
    this.store = store;
  }

  @Override
  protected ReadFieldResponse handle(ReadFieldRequestInternal internalRequest, Context context)
    throws Exception {
    validateFieldRequest(context, internalRequest);
    FieldRequest request = internalRequest.getBody();

    StoreClerk clerk = new StoreClerk(store.get(), internalRequest.getOrgId());
    String metricName = request.getMetricName();
    try {
      StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
      String cname = metric.getCanonicalNameFromUserFieldName(request.getFieldName());
      if (cname == null) {
        throw get40X(context, 4,
          format("No field with name or alias '%s' found", request.getFieldName()));
      }
      List<StoreClerk.Field> fields = metric.getUserVisibleFields();
      for (StoreClerk.Field field : fields) {
        if (field.getCname().equals(cname)) {
          return asResponse(field);
        }
      }
    } catch (SchemaNotFoundException e) {
      throw get40X(context, 4, format("Metric [%s] not found!", metricName));
    }
    throw new IllegalStateException(
      "Found a matching internal name, but when searching the field, couldn't find a matching "
      + "field!");
  }

  public static ReadFieldResponse asResponse(StoreClerk.Field field) {
    ReadFieldResponse response = new ReadFieldResponse();
    response.setName(field.getName());
    response.setAliases(field.getAliases().size() == 0 ?
                        EMPTY_STRING :
                        field.getAliases().toArray(EMPTY_STRING));
    response.setType(field.getType().name());
    return response;
  }
}
