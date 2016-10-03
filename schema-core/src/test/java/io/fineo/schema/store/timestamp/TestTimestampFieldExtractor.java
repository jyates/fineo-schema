package io.fineo.schema.store.timestamp;

import io.fineo.schema.MapRecord;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import io.fineo.schema.store.TimestampUtils;
import io.fineo.schema.timestamp.TimestampFieldExtractor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.fineo.schema.store.AvroSchemaProperties.TIMESTAMP_KEY;
import static io.fineo.schema.store.SchemaTestUtils.getStore;
import static org.junit.Assert.assertEquals;

public class TestTimestampFieldExtractor {

  private final String org = "org", metric = "metric";

  /**
   * If there is no alias, still read the timestamp from the timestamp field
   *
   * @throws Exception
   */
  @Test
  public void testNoAliasExtraction() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org).newMetric().setDisplayName(metric).build().commit();

    TimestampFieldExtractor extractor = getExtractor(store, org);
    Map<String, Object> map = new HashMap<>();
    map.put(TIMESTAMP_KEY, 1);
    assertEquals(TIMESTAMP_KEY, extractor.getTimestampKey(new MapRecord(map)));
  }

  /**
   * Provide a single alias and read the timestamp from that field
   */
  @Test
  public void testSingleAliasExtraction() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    String alias = "ta1";
    manager.newOrg(org).newMetric().setDisplayName(metric).build().commit();
    manager.updateOrg(org).updateMetric(metric).addFieldAlias(TIMESTAMP_KEY, alias)
           .build().commit();

    TimestampFieldExtractor extractor = getExtractor(store, org);
    Map<String, Object> map = new HashMap<>();
    map.put(alias, 1);
    assertEquals(alias, extractor.getTimestampKey(new MapRecord(map)));
  }


  /**
   * Provide a multiple aliases and read the timestamp from any of those fields (in specified order)
   */
  @Test
  public void testMultipleAliasExtraction() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    String alias = "ta1", alias2 = "ta2";
    manager.newOrg(org).newMetric().setDisplayName(metric).build().commit();
    manager.updateOrg(org).updateMetric(metric).addFieldAlias(TIMESTAMP_KEY, alias, alias2)
           .build().commit();

    TimestampFieldExtractor extractor = getExtractor(store, org);
    Map<String, Object> map = new HashMap<>();
    map.put(alias, 1);
    assertEquals(alias, extractor.getTimestampKey(new MapRecord(map)));

    map.remove(alias);
    map.put(alias2, 1);
    assertEquals(alias2, extractor.getTimestampKey(new MapRecord(map)));
  }

  private TimestampFieldExtractor getExtractor(SchemaStore store, String org)
    throws SchemaNotFoundException {
    StoreClerk clerk = new StoreClerk(store, org);
    return TimestampUtils.createExtractor(clerk.getMetricForUserNameOrAlias(metric));
  }
}
