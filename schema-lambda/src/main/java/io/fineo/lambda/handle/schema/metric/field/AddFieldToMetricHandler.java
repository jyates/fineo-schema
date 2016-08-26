package io.fineo.lambda.handle.schema.metric.field;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.fineo.lambda.handle.ThrowingRequestHandler;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.inject.SchemaStoreModule;
import io.fineo.schema.store.StoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayList;
import static io.fineo.lambda.handle.schema.inject.SchemaModulesUtil.validateFieldRequest;

/**
 * A lambda handler that handles Kinesis events
 */
public class AddFieldToMetricHandler
  extends ThrowingRequestHandler<AddFieldToMetricRequest, AddFieldToMetricResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(AddFieldToMetricHandler.class);

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
  public AddFieldToMetricResponse handle(AddFieldToMetricRequest input, Context context)
    throws Exception {
    validateFieldRequest(context, input);
    Preconditions.checkNotNull(input.getFieldType(),
      "Must specify a field type for the field: %s", input.getFieldName());
    StoreManager manager = store.get();
    return retry.run(() -> {
      StoreManager.NewFieldBuilder builder = manager.updateOrg(input.getOrgId())
                                                    .updateMetric(input.getMetricName())
                                                    .newField()
                                                    .withType(input.getFieldType())
                                                    .withName(input.getFieldName());
      if (input.getAliases() != null) {
        builder.withAliases(newArrayList(input.getAliases()));
      }
      builder.build().build().commit();
      return RESPONSE;
    });
  }
}
