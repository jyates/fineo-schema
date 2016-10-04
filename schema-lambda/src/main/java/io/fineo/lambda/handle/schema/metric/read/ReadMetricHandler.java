package io.fineo.lambda.handle.schema.metric.read;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.fineo.lambda.handle.external.ExternalErrorsUtil;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.field.read.ReadFieldHandler;
import io.fineo.lambda.handle.schema.field.read.ReadFieldResponse;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateMetricRequest;
import static java.lang.String.format;

/**
 * A lambda handler that handles Kinesis events
 */
public class ReadMetricHandler
  extends ExternalFacingRequestHandler<ReadMetricRequest, ReadMetricResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(ReadMetricHandler.class);
  private final Provider<SchemaStore> store;

  private static final String[] EMPTY = new String[0];

  @Inject
  public ReadMetricHandler(Provider<SchemaStore> store) {
    this.store = store;
  }

  @Override
  public ReadMetricResponse handle(ReadMetricRequest request, Context context)
    throws Exception {
    validateMetricRequest(context, request);

    StoreClerk clerk = new StoreClerk(store.get(), request.getOrgId());
    String metricName = request.getMetricName();
    try {
      StoreClerk.Metric metric = clerk.getMetricForUserNameOrAlias(metricName);
      ReadMetricResponse response = new ReadMetricResponse();

      response.setName(metric.getUserName());
      response.setAliases(metric.getAliases().size() == 0 ?
                          EMPTY : metric.getAliases().toArray(EMPTY));
      response.setTimestampPatterns(metric.getTimestampPatterns().size() == 0 ?
                                    EMPTY : metric.getTimestampPatterns().toArray(EMPTY));
      List<StoreClerk.Field> fields = metric.getUserVisibleFields();
      // make the timestamp visible to the user
      fields.add(metric.getTimestampField());
      ReadFieldResponse[] readFields = new ReadFieldResponse[fields.size()];
      response.setFields(readFields);
      for (int i = 0; i < readFields.length; i++) {
        readFields[i] = ReadFieldHandler.asResponse(fields.get(i));
      }
      LOG.debug("Returning: {}", response);
      return response;
    } catch (SchemaNotFoundException e) {
      throw ExternalErrorsUtil.get40X(context, 4, format("Metric [%s] not found!", metricName));
    }
  }
}
