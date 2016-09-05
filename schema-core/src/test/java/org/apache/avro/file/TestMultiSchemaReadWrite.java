package org.apache.avro.file;

import com.google.common.collect.Lists;
import io.fineo.internal.customer.BaseFields;
import io.fineo.schema.MapRecord;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.store.AvroSchemaProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static io.fineo.schema.store.AvroSchemaEncoder.asTypedRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that we can read/write an output stream with multiple schemas
 */
public class TestMultiSchemaReadWrite {

  private static final Log LOG = LogFactory.getLog(TestMultiSchemaReadWrite.class);

  @Test
  public void testSingleSchema() throws Exception {
    writeAndVerifyRecordsAndCodec(createRandomRecord());
  }

  @Test
  public void testTwoSchemas() throws Exception {
    writeAndVerifyRecordsAndCodec(createRandomRecord(), createRandomRecord());
  }

  @Test
  public void testManySchemas() throws Exception {
    GenericRecord[] records = new GenericRecord[5];
    for (int i = 0; i < records.length; i++) {
      records[i] = createRandomRecord();
    }
    writeAndVerifyRecordsAndCodec(records);
  }

  /**
   * Other tests cover just a single record in each schema, we use multiple schemas and multiple
   * records with each schema.
   *
   * @throws Exception
   */
  @Test
  public void testSeveralRecordsInEachSchema() throws Exception {
    List<GenericRecord> records = createRandomRecord(5);
    records.addAll(createRandomRecord(5));
    writeAndVerifyRecordsAndCodec(records.toArray(new GenericRecord[0]));
  }

  /**
   * List {@link #testSeveralRecordsInEachSchema()}, but the records are randomly sorted to
   * ensure that we switch between different writers seamlessly.
   *
   * @throws Exception on failure
   */
  @Test
  public void testSeveralRecordsInEachSchemaUnsorted() throws Exception {
    List<GenericRecord> records = createRandomRecord(5);
    records.addAll(createRandomRecord(5));
    records.addAll(createRandomRecord(5));
    Collections.shuffle(records);
    writeAndVerifyRecordsAndCodec(records.toArray(new GenericRecord[0]));
  }

  @Test
  public void testLengthChecks() throws Exception {
    GenericRecord record1 = createRandomRecord();
    GenericRecord record2 = createRandomRecord();
    GenericDatumWriter datumWriter = new GenericDatumWriter();
    MultiSchemaFileWriter writer = new MultiSchemaFileWriter(datumWriter);
    writer.create();
    assertEquals(4, writer.getBytesWritten());
    int record1Len = writer.append(record1).getBytesWritten();
    int record2Len = writer.append(record2).getBytesWritten();
    assertTrue("Didn't write more bytes after writing the second record", record2Len > record1Len);
    // we don't know how much metadata we will need to write for the records, but assume < 10% of
    // the total length of the byte array
    int sum = 4 /** magic */ + record2Len + 4 /** end identifier */;
    byte[] written = writer.close();
    int upper = (int) (sum * 0.1) + sum;
    assertTrue("Wrote bytes (" + written.length + ") outside expected range: " + sum + ", " + upper,
      sum < written.length && written.length <= upper);
  }


  private void writeAndVerifyRecordsAndCodec(GenericRecord... records)
    throws IOException {
    GenericDatumWriter datumWriter = new GenericDatumWriter();
    MultiSchemaFileWriter writer = new MultiSchemaFileWriter(datumWriter);
    writeAndVerifyRecords(writer, records);

//    writer = new MultiSchemaFileWriter(datumWriter);
//    writer.setCodec(CodecFactory.bzip2Codec());
//    writeAndVerifyRecords(writer, records);
  }

  /**
   * Actual work of writing, reading and verifying that the records match
   *
   * @param writer
   * @param records
   * @throws IOException
   */
  private void writeAndVerifyRecords(MultiSchemaFileWriter writer, GenericRecord... records)
    throws IOException {
    writer.create();
    for (GenericRecord record : records) {
      LOG.info("Wrote record: " + record + ", schema: " + record.getSchema());
      writer.append(record);
    }
    byte[] data = writer.close();

    // read back in the record
    SeekableByteArrayInput is = new SeekableByteArrayInput(data);
    MultiSchemaFileReader<GenericRecord> reader = new MultiSchemaFileReader(is);
    List<GenericRecord> recordList = Lists.newArrayList(records);
    LOG.info("Starting with expected records: " + recordList);
    for (int i = 0; i < records.length; i++) {
      GenericRecord record1 = reader.next();
      LOG.info("Got record: " + record1);
      assertTrue("remaining records: " + recordList + "\nmissing: " + record1,
        recordList.remove(record1));
    }
  }

  /**
   * Create a randomish schema using our schema generation utilities
   *
   * @return a record with a unique schema
   * @throws IOException
   */
  private GenericRecord createRandomRecord() throws IOException {
    return createRandomRecord(1).get(0);
  }

  private List<GenericRecord> createRandomRecord(int count) throws IOException {
    List<GenericRecord> records = new ArrayList<>(count);
    AvroSchemaInstanceBuilder builder = new AvroSchemaInstanceBuilder();
    // create a randomish name
    String name = UUID.randomUUID().toString();
    LOG.info("UUID: " + name);
    name = "a" + String.format("%x", new BigInteger(1, name.getBytes()));
    LOG.info("Record name: " + name);
    builder.withName(name).withNamespace("test_namespace");
    int fieldCount = new Random().nextInt(10);
    LOG.info("Field count: " + fieldCount);

    for (int i = 0; i < fieldCount; i++) {
      builder.newField().name("a" + i).type("boolean").done();
    }
    Schema schema = builder.build();

    // create a record with the schema
    for (int i = 0; i < count; i++) {
      BaseFields base = BaseFields.newBuilder()
                                  .setAliasName("somename_" + i)
                                  .setTimestamp(System.currentTimeMillis())
                                  .setUnknownFields(new HashMap<>())
                                  .build();
      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema)
        .set(AvroSchemaProperties.BASE_FIELDS_KEY, base);
      for (int j = 0; j < fieldCount; j++) {
        String fieldName = "a" + j;
        String alias = fieldName + "_alias";
        Map<String, Object> values = new HashMap<>();
        values.put(alias, true);
        GenericRecord fieldInst = asTypedRecord(schema, fieldName, alias, new MapRecord(values));
        recordBuilder.set(fieldName, fieldInst);
      }
      records.add(recordBuilder.build());
    }
    return records;
  }
}
