package io.fineo.schema;

import com.google.common.collect.ImmutableList;
import io.fineo.schema.avro.AvroSchemaEncoder;
import io.fineo.schema.avro.AvroSchemaManager;
import io.fineo.schema.store.SchemaStore;
import io.fineo.schema.store.StoreManager;
import io.fineo.schema.store.TestSchemaManager;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.ValidatorFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.ImmutableList.of;

/**
 *
 */
public class TestAvroSchemaEncoding {

  @Test
  public void testWrongTypedRecordField() throws Exception {
    SchemaStore store = getStore();
    StoreManager storeManager = new StoreManager(store);
    String org = "org", metric = "m1",
      f1 = "f1", f2 = "f2", f3 = "f3", f4 = "f4", f5 = "f5";
    TestSchemaManager.commitSimpleType(storeManager, org, metric, of(),
      p(f1, "DOUBLE"),
      p(f2, "BOOLEAN"),
      p(f3, "LONG"),
      p(f4, "FLOAT"),
      p(f5, "INTEGER"));

    Map<String, Object> mapRecord = new HashMap<>();
    mapRecord.put(AvroSchemaEncoder.ORG_ID_KEY, org);
    mapRecord.put(AvroSchemaEncoder.ORG_METRIC_TYPE_KEY, metric);
    mapRecord.put(AvroSchemaEncoder.TIMESTAMP_KEY, 10);
    mapRecord.put(f1, 1);
    mapRecord.put(f2, "true");
    mapRecord.put(f3, 1);
    mapRecord.put(f4, 1);
    mapRecord.put(f5, "1");

    AvroSchemaManager manager = new AvroSchemaManager(store, org);
    AvroSchemaEncoder encoder = manager.encode(metric);
    GenericData.Record record = encoder.encode(new MapRecord(mapRecord));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer);
    fileWriter.create(record.getSchema(), out);
    fileWriter.append(record);
    fileWriter.close();
    out.flush();
  }

  private <T, V> Pair<T, V> p(T t, V v) {
    return new Pair<>(t, v);
  }

  private SchemaStore getStore() {
    return new SchemaStore(new InMemoryRepository(ValidatorFactory.EMPTY));
  }
}
