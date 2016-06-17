package io.fineo.lambda.handle.schema;

import com.google.inject.Provider;
import io.fineo.lambda.handle.ThrowingRequestHandler;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.mockito.Mockito;

import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HandlerTestUtils {

  private HandlerTestUtils() {
  }

  public static <IN, OUT> void failNoValue(
    Function<Provider<StoreManager>, ThrowingRequestHandler<IN, OUT>> handler, IN msg) {
    Provider manager = Mockito.mock(Provider.class);
    ThrowingRequestHandler<IN, OUT> handle = handler.apply(manager);
    try {
      handle.handleRequest(msg, null);
      fail();
    } catch (NullPointerException e) {
      Mockito.verifyZeroInteractions(manager);
    }
  }

  public static StoreClerk.Field getOnlyFirstField(String org, String metric, SchemaStore store) {
    StoreClerk clerk = new StoreClerk(store, org);
    List<StoreClerk.Metric> metrics = clerk.getMetrics();
    assertEquals("Got wrong number of metrics! " + metrics, 1, metrics.size());
    StoreClerk.Metric m = metrics.get(0);
    assertEquals(metric, m.getUserName());
    List<StoreClerk.Field> fields = m.getUserVisibleFields();
    assertEquals(1, fields.size());
    return fields.get(0);
  }
}
