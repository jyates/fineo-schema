package io.fineo.schema.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import io.fineo.aws.AwsDependentTests;
import io.fineo.aws.rule.AwsCredentialResource;
import javafx.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.schemarepo.AbstractTestPersistentRepository;
import org.schemarepo.Repository;
import org.schemarepo.SchemaEntry;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;
import org.schemarepo.SubjectConfig;
import org.schemarepo.ValidatorFactory;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test the {@link DynamoDBRepository} against a local dynamodb instance (no network overhead).
 */
@Category(AwsDependentTests.class)
public class TestDynamoDBRepository extends AbstractTestPersistentRepository<DynamoDBRepository> {

  private static final Log LOG = LogFactory.getLog(TestDynamoDBRepository.class);
  private static String testTableName = "schema-repository-test-" + UUID.randomUUID().toString();

  private static AmazonDynamoDBClient dynamodb;
  private static DynamoDBProxyServer server;

  @ClassRule
  public static AwsCredentialResource credentials = new AwsCredentialResource();
  private static int port;

  @BeforeClass
  public static void setupDb() throws Exception {
    // create a local database instance with an local server url on an open port
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    final String[] localArgs = {"-inMemory", "-port", String.valueOf(port)};
    server = ServerRunner.createServerFromCommandLineArgs(localArgs);
    server.start();
    dynamodb = createClient();

    for (String table : dynamodb.listTables().getTableNames()) {
      System.out.println(table);
    }
  }

  private static AmazonDynamoDBClient createClient() {
    AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials.getFakeProvider());
    client.setEndpoint("http://localhost:"+port);
    return client;
  }

  @After
  public void tearDownRepository() throws Exception {
    dynamodb.deleteTable(testTableName);
  }


  @AfterClass
  public static void shutdown() throws Exception {
    server.stop();
    dynamodb.shutdown();
  }

  @Override
  protected DynamoDBRepository createRepository() throws Exception {
    return createRepository(dynamodb);
  }

  private DynamoDBRepository createRepository(AmazonDynamoDBClient client) {
    return new DynamoDBRepository(client, testTableName, new ValidatorFactory.Builder().build());
  }

  @Test
  public void testSlowRegistrationLatestOrdering() throws Exception {
    InjectedRepository injected1  = new InjectedRepository();
    InjectedRepository injected2 = new InjectedRepository();

    Pair<Subject, Subject> subjects = createSubjectsAndVerify(injected1, injected2);
    Subject subject = subjects.getKey();
    Subject subject2 = subjects.getValue();

    SchemaEntry entry = registerNewEntity(subjects);

    // release latch when we start making the update, but then hold it until after we make a
    // different update
    CountDownLatch firstUpdateCommit = new CountDownLatch(1);
    CountDownLatch firstUpdateStarted = new CountDownLatch(1);
    injected1.faults.addTracking(UpdateItemRequest.class, firstUpdateStarted);
    injected1.faults.addSlowDown(UpdateItemRequest.class, firstUpdateCommit);

    Thread t = new Thread(() -> {
      try {
        subject.registerIfLatest("newschema1", entry);
      } catch (SchemaValidationException e) {
        throw new RuntimeException(e);
      } finally {
        LOG.info("Subject update done!");
      }
    });
    t.start();

    // wait until right before we make the first update
    firstUpdateStarted.await();

    // make the second update
    subject2.registerIfLatest("newschema2", entry);
    firstUpdateCommit.countDown();

    LOG.info("Waiting for other schema registration to complete...");
    t.join();

    // ensure we get three schemas, in the order we expect
    Map<String, String> schemas = new HashMap<>();
    schemas.put("0", "someschema");
    schemas.put("1", "newschema2");
    assertSchemasMatch(schemas, subject);
    assertSchemasMatch(schemas, subject2);
  }

  /**
   * Ensure that when we register a schema, it actually registers the correct value when there is
   * a conflicting update
   *
   * @throws Exception on failure
   */
  @Test
  public void testSlowRegistrationOrdering() throws Exception {
    InjectedRepository injected1  = new InjectedRepository();
    InjectedRepository injected2 = new InjectedRepository();

    Pair<Subject, Subject> subjects = createSubjectsAndVerify(injected1, injected2);
    Subject subject = subjects.getKey();
    Subject subject2 = subjects.getValue();

    SchemaEntry entry = registerNewEntity(subjects);

    // release latch when we start making the update, but then hold it until after we make a
    // different update
    CountDownLatch firstUpdateCommit = new CountDownLatch(1);
    CountDownLatch firstUpdateStarted = new CountDownLatch(1);
    injected1.faults.addTracking(UpdateItemRequest.class, firstUpdateStarted);
    injected1.faults.addSlowDown(UpdateItemRequest.class, firstUpdateCommit);

    Thread t = new Thread(() -> {
      try {
        subject.register("newschema1");
      } catch (SchemaValidationException e) {
        throw new RuntimeException(e);
      } finally {
        LOG.info("Subject update done!");
      }
    });
    t.start();

    // wait until right before we make the first update
    firstUpdateStarted.await();

    // make the second update
    subject2.registerIfLatest("newschema2", entry);
    firstUpdateCommit.countDown();

    LOG.info("Waiting for other schema registration to complete...");
    t.join();

    // ensure we get three schemas, in the order we expect
    Map<String, String> schemas = new HashMap<>();
    schemas.put("0", "someschema");
    schemas.put("1", "newschema2");
    schemas.put("2", "newschema1");
    assertSchemasMatch(schemas, subject);
    assertSchemasMatch(schemas, subject2);
  }

  @Test
  public void testDoubleRegister() throws Exception{
    Repository repo = createRepository();
    Repository repo2 = createRepository();
    String subject = "dsfds";
    repo.register(subject, null);
    SubjectConfig config = new SubjectConfig.Builder().set("k", "v").build();
    repo2.register(subject, config);
    Subject s  = repo.lookup(subject);
    assertEquals(0, s.getConfig().asMap().size());
    assertEquals(0, repo2.lookup(subject).getConfig().asMap().size());
  }

  /**
   * add schema to one and ensure it shows up in the other
   * @param subjects
   * @return
   * @throws SchemaValidationException
   */
  private SchemaEntry registerNewEntity(Pair<Subject, Subject> subjects)
    throws SchemaValidationException {
    SchemaEntry entry = subjects.getKey().register("someschema");
    assertEquals(entry, subjects.getKey().lookupById(entry.getId()));
    assertEquals(entry, subjects.getValue().lookupById(entry.getId()));
    assertEquals(entry, subjects.getValue().lookupBySchema(entry.getSchema()));
    return entry;
  }

  private Pair<Subject, Subject> createSubjectsAndVerify(InjectedRepository injected1,
    InjectedRepository injected2) {
    // basic setup by adding a subject with an empty config
    String subjectName = "sub1";
    injected1.repo.registerSubjectInBackend(subjectName, null);
    Subject subject = injected1.repo.getSubjectInstance(subjectName);
    assertNotNull(subject);
    Subject subject2 = injected2.repo.getSubjectInstance(subjectName);
    assertNotNull(subject2);
    return new Pair<>(subject, subject2);
  }

  private void assertSchemasMatch(Map<String, String> schemas , Subject subject){
    int i = schemas.size() - 1;
    for (SchemaEntry schemaEntry : subject.allEntries()) {
      String key = String.valueOf(i--);
      assertEquals(key, schemaEntry.getId());
      assertEquals(schemas.get(key), schemaEntry.getSchema());
    }
  }

  private class InjectedRepository{
    private final DynamoDBRepository repo;
    private final FaultInjectionHandler faults;
    private final AmazonDynamoDBClient client;

    public InjectedRepository(){
      this.client = createClient();
      this.faults = new FaultInjectionHandler();
      client.addRequestHandler(faults);
      this.repo = createRepository(client);
    }
  }

  @Test
  public void testDynamicItems() throws Exception {
    DynamoDBRepository repo = createRepository();
    String subject = UUID.randomUUID().toString();
    String col1 = "col1", col2 = "col2";
    String t1 = "t1", t2 = "t2";

    // add a single column
    repo.addDynamicColumn(subject, col1, t1);
    Map<String, Set<String>> dynamic = repo.getDynamicColumns(subject);
    assertEquals(1, dynamic.size());
    Set<String> types = new HashSet<>();
    types.add(t1);
    Map<String, Set<String>> expected = new HashMap<>();
    expected.put(col1, types);
    assertEquals(expected, dynamic);

    // add the second set of column type
    repo.addDynamicColumn(subject, col1, t2);
    dynamic = repo.getDynamicColumns(subject);
    types.add(t2);
    assertEquals(1, dynamic.size());
    assertEquals(expected, dynamic);

    // add a new column and type
    repo.addDynamicColumn(subject, col2, t1);
    dynamic = repo.getDynamicColumns(subject);
    assertEquals(2, dynamic.size());
    Set<String> types2 = new HashSet<>();
    types2.add(t1);
    expected.put(col2, types2);
    assertEquals(expected, dynamic);
  }
}
