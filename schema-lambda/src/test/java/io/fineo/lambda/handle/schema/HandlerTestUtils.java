package io.fineo.lambda.handle.schema;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import io.fineo.lambda.handle.ThrowingRequestHandler;
import io.fineo.lambda.handle.external.ExternalFacingRequestHandler;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.codehaus.jackson.JsonParseException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HandlerTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HandlerTestUtils.class);

  private HandlerTestUtils() {
  }

  public static <IN, OUT> void failNoValue(
    Function<Provider<StoreManager>, ExternalFacingRequestHandler<IN, OUT>> handler, IN msg)
    throws Exception {
    failNoValueWithProvider(handler, msg);
  }

  public static <IN, OUT, PROVIDED> void failNoValueWithProvider(
    Function<Provider<PROVIDED>, ExternalFacingRequestHandler<IN, OUT>> handler, IN msg)
    throws Exception {
    Provider manager = Mockito.mock(Provider.class);
    ThrowingRequestHandler<IN, OUT> handle = handler.apply(manager);
    Context context = Mockito.mock(Context.class);
    Mockito.when(context.getAwsRequestId()).thenReturn(UUID.randomUUID().toString());
    try {
      handle.handleRequest(msg, context);
      fail();
    } catch (RuntimeException e) {
      expectError(e, 400, "Bad Request");
      Mockito.verifyZeroInteractions(manager);
    }
  }

  public static <IN, OUT, PROVIDED> void fail500(
    Function<Provider<PROVIDED>, ExternalFacingRequestHandler<IN, OUT>> handler, IN msg)
    throws Exception {
    Provider manager = Mockito.mock(Provider.class);
    ThrowingRequestHandler<IN, OUT> handle = handler.apply(manager);
    Context context = Mockito.mock(Context.class);
    Mockito.when(context.getAwsRequestId()).thenReturn(UUID.randomUUID().toString());
    try {
      handle.handleRequest(msg, context);
      fail();
    } catch (RuntimeException e) {
      expectError(e, 500, "Internal Server Error");
      assertTrue("Got a 500 error, but doesn't contain the error email!",
        e.getMessage().contains("errors@fineo.io"));
      Mockito.verifyZeroInteractions(manager);
    }
  }

  public static Map<String, Object> unwrapException(Exception e) throws Exception {
    if (e instanceof RuntimeException) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        return mapper.readValue(e.getMessage(), Map.class);
      } catch (JsonParseException | NullPointerException jpe) {
        LOG.error("Failed to read error message!");
        throw e;
      }
    }

    throw e;
  }

  public static void expectError(Exception e, int code, String type) throws Exception {
    Map<String, Object> error = unwrapException(e);
    assertEquals("Got error: " + error, code, error.get("httpStatus"));
    assertEquals("Got error: " + error, type, error.get("errorType"));
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
