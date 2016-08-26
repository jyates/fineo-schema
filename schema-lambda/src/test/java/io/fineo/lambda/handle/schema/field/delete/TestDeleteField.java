package io.fineo.lambda.handle.schema.field.delete;

import com.google.inject.Provider;
import io.fineo.lambda.handle.schema.UpdateRetryer;
import io.fineo.lambda.handle.schema.create.TestCreateOrg;
import io.fineo.lambda.handle.schema.metric.create.TestCreateMetric;
import io.fineo.lambda.handle.schema.metric.field.TestAddField;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

public class TestDeleteField {

  @Test
  public void testDelete() throws Exception {
    String org = "org", metric = "metric", field = "field", type = "STRING";
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    TestCreateOrg.createOrg(store, org);
    TestCreateMetric.createMetric(store, org, metric);
    TestAddField.createField(store, org, metric, field, type);

    DeleteFieldHandler handler = handler(() -> new StoreManager(store));
    DeleteFieldRequest request = new DeleteFieldRequest();
    request.setOrgId(org);
    request.setMetricName(metric);
    request.setFieldName(field);
    handler.handle(request, null);

    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals(1, metrics.size());
    StoreClerk.Metric m = metrics.get(0);
    assertEquals(newArrayList(), m.getUserVisibleFields());
  }

  private DeleteFieldHandler handler(Provider<StoreManager> provider) {
    return new DeleteFieldHandler(provider, new UpdateRetryer(), 1);
  }
}
