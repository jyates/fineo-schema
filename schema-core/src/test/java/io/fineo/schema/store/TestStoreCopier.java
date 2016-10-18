package io.fineo.schema.store;

import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

public class TestStoreCopier {

  private static final String ORG_ID = "orgId";
  private static final String NEW_SCHEMA_DISPLAY_NAME = "newschema";
  private static final String BOOLEAN_FIELD_NAME = "bField";

  @Test
  public void testCopySingleOrg() throws Exception {
    SchemaBuilder.Organization org =
      SchemaBuilder.create().newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
                   .withBoolean(BOOLEAN_FIELD_NAME).asField().build().build();
    InMemoryRepository fromRepo = new InMemoryRepository(ValidatorFactory.EMPTY);
    InMemoryRepository toRepo = new InMemoryRepository(ValidatorFactory.EMPTY);
    // setup the org in the store
    SchemaStore from = new SchemaStore(fromRepo);
    from.createNewOrganization(org);

    SchemaStore to = new SchemaStore(toRepo);
    StoreCopier copier = new StoreCopier(from, to);
    copier.copy(ORG_ID);
    TestSchemaStore.verifySchemasMatch(from, to, ORG_ID);
  }

  @Test
  public void testCopyMultipleOrgs() throws Exception {
    String orgId2 = ORG_ID + "_2";
    SchemaBuilder.Organization org =
      SchemaBuilder.create().newOrg(ORG_ID).newMetric().withName(NEW_SCHEMA_DISPLAY_NAME)
                   .withBoolean(BOOLEAN_FIELD_NAME).asField().build().build();
    SchemaBuilder.Organization org2 =
      SchemaBuilder.create().newOrg(orgId2).newMetric().withName("metric2")
                   .withBytes("bytes").asField().build().build();

    InMemoryRepository fromRepo = new InMemoryRepository(ValidatorFactory.EMPTY);
    SchemaStore from = new SchemaStore(fromRepo);
    from.createNewOrganization(org);
    from.createNewOrganization(org2);

    InMemoryRepository toRepo = new InMemoryRepository(ValidatorFactory.EMPTY);
    SchemaStore to = new SchemaStore(toRepo);
    StoreCopier copier = new StoreCopier(from, to);
    copier.copy(ORG_ID);
    copier.copy(orgId2);

    TestSchemaStore.verifySchemasMatch(from, to, ORG_ID);
    TestSchemaStore.verifySchemasMatch(from, to, orgId2);
  }
}
