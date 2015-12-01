package org.apache.avro.file;

import io.fineo.avro.writer.MultiContents;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A light wrapper around a standard Avro file writer that will write a multiple schemas to an
 * in-memory output stream. All data is buffered in memory, so you need to be careful to check
 * the size periodically to make sure you have enough memory.
 */
public class MultiSchemaStreamWriter<D extends GenericRecord> {
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final DatumWriter<D> writer;
  private CodecFactory codec;
  private boolean isOpen = false;
  private int bytesCount = 0;
  private Map<Schema, Writer> writers = new HashMap<>();
  private List<Long> offsets = new ArrayList<>();

  public MultiSchemaStreamWriter(DatumWriter<D> datumWriter) {
    this.writer = datumWriter;
  }

  public MultiSchemaStreamWriter setCodec(CodecFactory c) {
    assertNotOpen();
    this.codec = c;
    return this;
  }

  public int getBytesWritten() {
    return bytesCount;
  }

  public MultiSchemaStreamWriter create() throws IOException {
    this.isOpen = true;
    out.write(MultiSchemaData.MAGIC);
    return this;
  }

  public MultiSchemaStreamWriter append(D record) throws IOException {
    assertOpen();
    Schema schema = record.getSchema();
    Writer writer = writers.get(schema);
    if (writer == null) {
      writer = createWriter(schema);
      writers.put(schema, writer);
    }
    writer.append(record);
    return this;
  }

  private Writer createWriter(Schema schema) throws IOException {
    return new Writer(schema, createDataFileWriter());
  }

  private DataFileWriter<D> createDataFileWriter() {
    DataFileWriter<D> writer = new DataFileWriter<>(this.writer);
    if (codec != null) {
      writer.setCodec(codec);
    }
    return writer;
  }

  public byte[] close() throws IOException {
    this.isOpen = false;
    // close and flush any open data
    for (Writer writer : writers.values()) {
      int length = writer.close(out);
      addMetadata(length);
    }
    // append the field map
    MultiContents meta = new MultiContents(this.offsets);
    SpecificDatumWriter<MultiContents> writer = new SpecificDatumWriter<>(MultiContents.class);
    int start = out.size();
    Encoder enc = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(meta, enc);
    enc.flush();

    DataOutputStream dout = new DataOutputStream(out);
    dout.writeInt(start);
    dout.close();
    out.close();
    return out.toByteArray();
  }

  private void addMetadata(long length) {
    offsets.add(length);
  }

  private void assertOpen() {
    if (!isOpen)
      throw new AvroRuntimeException("not open");
  }

  private void assertNotOpen() {
    if (isOpen)
      throw new AvroRuntimeException("already open");
  }

  private class Writer {
    private final Schema schema;
    DataFileWriter<D> writer;
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    public Writer(Schema schema, DataFileWriter<D> writer) throws IOException {
      this.schema = schema;
      this.writer = writer;
      writer.create(schema, out);
    }

    public int close(ByteArrayOutputStream destination) throws IOException {
      writer.close();
      out.close();
      destination.write(out.toByteArray());
      return out.size();
    }

    public void append(D record) throws IOException {
      // set the schema to write with
      MultiSchemaStreamWriter.this.writer.setSchema(schema);
      writer.append(record);
      //immediately flush
      writer.flush();
    }
  }
}
