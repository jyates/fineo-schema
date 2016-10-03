package io.fineo.schema.store.timestamp;

import io.fineo.schema.MapRecord;
import io.fineo.schema.exception.SchemaNotFoundException;
import io.fineo.schema.store.AvroSchemaProperties;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreClerk;
import io.fineo.schema.store.StoreManager;
import io.fineo.schema.store.TimestampUtils;
import io.fineo.schema.timestamp.MultiLevelTimestampParser;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.fineo.schema.store.SchemaTestUtils.getStore;
import static io.fineo.schema.timestamp.MultiPatternTimestampParser.TimeFormats.ISO_INSTANT;
import static io.fineo.schema.timestamp.MultiPatternTimestampParser.TimeFormats.RFC_1123_DATE_TIME;
import static org.junit.Assert.assertEquals;

public class TestMultiPatternTimestampFormatParser {

  private final String fixedTsString = "Tue, 06 Sep 2016 19:00:46 GMT";
  private final long fixedTs = 1473188446000l;

  private final String org = "org", metric = "metric";

  @Test
  public void testFallbackToLong() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org).newMetric().setDisplayName(metric).build().commit();

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.TIMESTAMP_KEY, 1);
    assertEquals(1L, parse(map, store));
  }

  @Test
  public void testMetricLevelParsing() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org)
           .newMetric().setDisplayName(metric).withTimestampFormat(RFC_1123_DATE_TIME.name())
           .build().commit();

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.TIMESTAMP_KEY, fixedTsString);
    assertEquals(fixedTs, parse(map, store));
  }

  @Test
  public void testOrgLevelParsing() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org)
           .withTimestampFormat(RFC_1123_DATE_TIME.name())
           .newMetric().setDisplayName(metric)
           .build().commit();

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.TIMESTAMP_KEY, fixedTsString);
    assertEquals(fixedTs, parse(map, store));
  }

  @Test
  public void testFallThroughMetricToOrgParsing() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org)
           .withTimestampFormat(RFC_1123_DATE_TIME.name())
           .newMetric().setDisplayName(metric)
           .withTimestampFormat(ISO_INSTANT.name())
           .build().commit();

    Map<String, Object> map = new HashMap<>();
    map.put(AvroSchemaProperties.TIMESTAMP_KEY, fixedTsString);
    assertEquals(fixedTs, parse(map, store));
  }

  @Test
  public void testParsesAliasedTimestampField() throws Exception {
    SchemaStore store = getStore();
    StoreManager manager = new StoreManager(store);
    manager.newOrg(org)
           .newMetric().setDisplayName(metric)
           .build().commit();
    String alias = "alias";
    manager.updateOrg(org).updateMetric(metric).addFieldAlias(AvroSchemaProperties.TIMESTAMP_KEY,
      alias).build().commit();

    Map<String, Object> map = new HashMap<>();
    map.put(alias, fixedTs);
    assertEquals(fixedTs, parse(map, store));
  }

  private long parse(Map<String, Object> map, SchemaStore store) throws SchemaNotFoundException {
    StoreClerk clerk = new StoreClerk(store, org);
    StoreClerk.Metric m = clerk.getMetricForUserNameOrAlias(metric);
    MultiLevelTimestampParser parser = new MultiLevelTimestampParser(m.getTimestampPatterns(), clerk
      .getOrgMetadataForTesting().getTimestampFormats(), TimestampUtils.createExtractor(m));

    return parser.getTimestamp(new MapRecord(map));
  }
}
