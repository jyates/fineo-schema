package io.fineo.lambda.handle.schema.metric.field;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.client.model.schema.field.CreateFieldRequest;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.checkNotNull;
import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateFieldRequest;
import static io.fineo.lambda.handle.schema.inject.SchemaHandlerUtil.validateRequest;

/**
 * A lambda handler that handles Kinesis events
 */
public class AddFieldToMetricHandler
  extends ExternalFacingRequestHandler<AddFieldToMetricRequestInternal, AddFieldToMetricResponse> {

  private static final AddFieldToMetricResponse RESPONSE = new AddFieldToMetricResponse();
  private final Provider<StoreManager> store;
  private final UpdateRetryer retry;

  @Inject
  public AddFieldToMetricHandler(Provider<StoreManager> manager,
    UpdateRetryer retryer,
    @Named(SchemaStoreModule.SCHEMA_UPDATE_RETRIES) int retries) {
    this.store = manager;
    this.retry = retryer;
    this.retry.setRetries(retries);
  }


  @Override
  public AddFieldToMetricResponse handle(AddFieldToMetricRequestInternal irequest, Context context)
    throws Exception {
    CreateFieldRequest request = irequest.getBody();
    validateRequest(context, irequest);
    validateFieldRequest(context, request);

    checkNotNull(context, request.getFieldType(), "Must specify a field type for the field: %s",
      request.getFieldName());
    StoreManager manager = store.get();
    return retry.run(() -> {
      StoreManager.NewFieldBuilder builder = manager.updateOrg(irequest.getOrgId())
                                                    .updateMetric(request.getMetricName())
                                                    .newField()
                                                    .withType(request.getFieldType())
                                                    .withName(request.getFieldName());
      if (request.getAliases() != null) {
        builder.withAliases(newArrayList(request.getAliases()));
      }
      builder.build().build().commit();
      return RESPONSE;
    }, context);
  }
}
