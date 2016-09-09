package io.fineo.lambda.handle.schema.create;

import com.amazonaws.services.lambda.runtime.Context;
import io.fineo.lambda.handle.schema.HandlerTestUtils;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.exception.SchemaExistsException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import org.junit.Test;
import org.mockito.Mockito;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCreateOrg {

  @Test
  public void testCreateOrg() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    createOrg(store, org);
    assertEquals(of(), new StoreClerk(store, org).getMetrics());
  }

  @Test
  public void testNoOrgId() throws Exception {
    CreateOrgRequest request = new CreateOrgRequest();
    HandlerTestUtils.failNoValue(p -> new CreateOrgHandler(p), request);

    request.setOrgId("");
    HandlerTestUtils.failNoValue(p -> new CreateOrgHandler(p), request);
  }

  @Test
  public void testFailToCreateOrgTwice() throws Exception {
    SchemaStore store = new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
    String org = "org1";
    createOrg(store, org);
    boolean found = true;
    try {
      createOrg(store, org);
      fail();
    } catch (RuntimeException e) {
      HandlerTestUtils.expectError(e, 400, "Bad Request");
      Map<String, Object> info = HandlerTestUtils.unwrapException(e);
      assertTrue(((String) info.get("message")).contains("org1 already exists"));
      found = true;
    }
    assertTrue("Didn't throw a " + SchemaExistsException.class + " on second create attempt",
      found);
  }

  public static void createOrg(SchemaStore store, String org)
    throws IOException, OldSchemaException {
    StoreManager manager = new StoreManager(store);
    CreateOrgHandler handler = new CreateOrgHandler(() -> manager);
    CreateOrgRequest request = new CreateOrgRequest();
    request.setOrgId(org);
    Context context = Mockito.mock(Context.class);
    Mockito.when(context.getAwsRequestId()).thenReturn("someid");
    assertNotNull(handler.handle(request, context));
  }
}
