package org.apache.avro.file;

import io.fineo.avro.writer.MultiContents;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The counterpoint to the {@link MultiSchemaFileWriter}
 */
public class MultiSchemaFileReader<D> {
  private static final Log LOG = LogFactory.getLog(MultiSchemaFileReader.class);
  private final SeekableInput input;
  private final GenericDatumReader<D> datum;
  private List<Block> blocks;
  private Block currentBlock;

  public MultiSchemaFileReader(SeekableInput input)
    throws IOException {
    this.input = input;
    initialize(input);
    this.datum = new GenericDatumReader<>();
  }

  private void initialize(SeekableInput input) throws IOException {
    // ensure that the magic is the first few bytes
    byte[] magic = new byte[MultiSchemaData.MAGIC.length];
    input.read(magic, 0, magic.length);
    if (!Arrays.equals(magic, MultiSchemaData.MAGIC)) {
      throw new IllegalArgumentException("File is not a mutli-schema file!");
    }

    // seek to the end and read in a integer
    long length = input.length();
    input.seek(length - MultiSchemaData.OFFSET_COUNT_LENGTH);

    // read in the offset of the metadata
    byte[] bytes = new byte[MultiSchemaData.OFFSET_COUNT_LENGTH];
    input.read(bytes, 0, bytes.length);
    // bytebuffer is better than DataInputStream here b/c DIS chokes on some lengths when reading
    // back 4 bytes...yeah, I dunno.
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    int metaOffset = buf.getInt();
    input.seek(metaOffset);

    // read in the metadata
    MultiContents meta = readOffsets(input);
    blocks = new ArrayList<>(meta.getOffsets().size());
    // first offset skips past the magic
    long start = magic.length;
    // first offset skips past the magic
    for (int i = 0; i < meta.getOffsets().size(); i++) {
      long offset = meta.getOffsets().get(i);
      blocks.add(new Block(start, offset));
      start += offset;
    }
    // seek back to the beginning of the file
    input.seek(magic.length);
  }

  private MultiContents readOffsets(SeekableInput input) throws IOException {
    InputStream wis = new WrapperInputStream(input);
    SpecificDatumReader<MultiContents> contents =
      new SpecificDatumReader<>(MultiContents.getClassSchema());
    Decoder dec = DecoderFactory.get().binaryDecoder(wis, null);
    return contents.read(null, dec);
  }

  private class WrapperInputStream extends InputStream {

    private final SeekableInput delegate;
    private byte[] oneByte = new byte[1];

    public WrapperInputStream(SeekableInput input) {
      this.delegate = input;
    }

    @Override
    public int read() throws IOException {
      int n = delegate.read(oneByte, 0, 1);
      if (n == 1) {
        return oneByte[0] & 0xff;
      } else {
        return n;
      }
    }
  }

  public D next() throws IOException {
    return next(null);
  }

  public D next(D reuse) throws IOException {
    getNextBlock();
    // no more blocks, done!
    if (currentBlock == null) {
      input.close();
      return null;
    }
    return currentBlock.next(reuse);
  }

  private void getNextBlock() throws IOException {
    if (currentBlock == null) {
      if (blocks.size() == 0) {
        return;
      }
      currentBlock = blocks.remove(0);
      LOG.info("Moving to next block: " + currentBlock);
      currentBlock.open(input);
    }

    // skip to the oneByte block of this one is exhausted
    if (currentBlock.exhausted()) {
      currentBlock = null;
      getNextBlock();
    }
  }

  private class Block {

    private final long offset;
    private final long length;
    private DataFileReader<D> reader;
    private SeekableInput limited;

    public Block(long offset, Long length) {
      this.offset = offset;
      this.length = length;
    }

    public boolean exhausted() throws IOException {
      return !reader.hasNext();
    }

    public void open(SeekableInput input) throws IOException {
      // setup a new reader using the same datum reader
      this.limited = new TranslatedSeekableInput(offset, length, input);
      // have to remove any assumptions about the expected schema because we are changing schemas
      datum.setExpected(null);
      reader = new DataFileReader<D>(limited, datum);
    }

    public D next(D reuse) throws IOException {
      D next = reader.next(reuse);
      return next;
    }

    @Override
    public String toString() {
      return "Block{" +
             "offset=" + offset +
             ", length=" + length +
             '}';
    }
  }
}
