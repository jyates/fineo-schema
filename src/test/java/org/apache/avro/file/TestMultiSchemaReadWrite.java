package org.apache.avro.file;

import com.google.common.collect.Lists;
import io.fineo.schema.avro.AvroSchemaInstanceBuilder;
import io.fineo.schema.store.SchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * Test that we can read/write an output stream with multiple schemas
 */
public class TestMultiSchemaReadWrite {

  private static final Log LOG = LogFactory.getLog(TestMultiSchemaReadWrite.class);

  @Test
  public void testSingleSchema() throws Exception {
    GenericDatumWriter datumWriter = new GenericDatumWriter();
    MultiSchemaFileWriter writer = new MultiSchemaFileWriter(datumWriter);
    writeAndVerifyRecords(writer, createRandomRecord());
  }

  @Test
  public void testSingleSchemaWithCodec() throws Exception {
    GenericDatumWriter datumWriter = new GenericDatumWriter();
    MultiSchemaFileWriter writer = new MultiSchemaFileWriter(datumWriter);
    writer.setCodec(CodecFactory.bzip2Codec());
    writeAndVerifyRecords(writer, createRandomRecord());
  }

  @Test
  public void testTwoSchemas() throws Exception {
    GenericDatumWriter datumWriter = new GenericDatumWriter();
    MultiSchemaFileWriter writer = new MultiSchemaFileWriter(datumWriter);
    writeAndVerifyRecords(writer, createRandomRecord(), createRandomRecord());
  }

  @Test
  public void testManySchemas() throws Exception {
    GenericDatumWriter datumWriter = new GenericDatumWriter();
    MultiSchemaFileWriter writer = new MultiSchemaFileWriter(datumWriter);
    GenericRecord[] records = new GenericRecord[5];
    for (int i = 0; i < records.length; i++) {
      records[i] = createRandomRecord();
    }
    writeAndVerifyRecords(writer, records);
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
    for (int i = 0; i < records.length; i++) {
      GenericRecord record1 = reader.next();
      LOG.info("Got record: " + record1);
      assertTrue("records list: " + recordList + "\nmissing: " + record1,
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
    AvroSchemaInstanceBuilder builder = new AvroSchemaInstanceBuilder();
    // create a randomish name
    String name = UUID.randomUUID().toString();
    //String name = nameIterator.next();
    LOG.info("UUID: " + name);
    name = "a" + String.format("%x", new BigInteger(1, name.getBytes()));
    LOG.info("Record name: " + name);
    builder.withName(name).withNamespace("ns");
    int fieldCount = new Random().nextInt(10);
    //int fieldCount = countIterator.next();
    LOG.info("Field count: " + fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      builder.newField().name("a" + i).type("boolean").done();
    }
    Schema schema = builder.build();

    // create a record with the schema
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema)
      .set(SchemaBuilder.UNKNOWN_KEYS_FIELD, new HashMap<>());
    for (int i = 0; i < fieldCount; i++) {
      recordBuilder.set("a" + i, true);
    }
    return recordBuilder.build();
  }
}
