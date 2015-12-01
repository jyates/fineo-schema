package org.apache.avro.file;

import io.fineo.avro.writer.MultiContents;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The counterpoint to the {@link MultiSchemaStreamWriter}
 */
public class MultiSchemaReader<D> {
  private final SeekableInput input;
  private final DatumReader<D> datumReader;
  private List<Block> blocks;
  private Block currentBlock;

  public MultiSchemaReader(SeekableInput input, GenericDatumReader<D> datumReader)
    throws IOException {
    this.input = input;
    this.datumReader = datumReader;
    initialize(input);
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
    InputStream wis = new WrapperInputStream(input);
    SpecificDatumReader<MultiContents> contents =
      new SpecificDatumReader<>(MultiContents.getClassSchema());
    Decoder dec = DecoderFactory.get().binaryDecoder(wis, null);
    MultiContents offsets = contents.read(null, dec);
    List<Long> meta = offsets.getOffsets();
    blocks = new ArrayList<>(meta.size());
    // first offset skips past the magic
    long start = magic.length;
    // first offset skips past the magic
    for (Long offset : meta) {
      blocks.add(new Block(start, offset));
      start += offset;
    }

    // seek back to the beginning of the file
    input.seek(magic.length);
  }

  private class WrapperInputStream extends InputStream {

    private final SeekableInput delegate;
    private byte[] next = new byte[1];

    public WrapperInputStream(SeekableInput input) {
      this.delegate = input;
    }

    @Override
    public int read() throws IOException {
      if (delegate.read(next, 0, 1) < 0) {
        return -1;
      }
      return next[0];
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
      currentBlock.open(input);
    }

    // skip to the next block of this one is exhausted
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
      return !this.reader.hasNext();
    }

    public void open(SeekableInput input) throws IOException {
      this.limited = new TranslatedSeekableInput(offset, length, input);
      reader = new DataFileReader<D>(limited, datumReader);
    }

    public D next(D reuse) {
      return reader.next();
    }

    private class TranslatedSeekableInput implements SeekableInput {
      private final long length;
      private final SeekableInput delegate;
      private final long offset;

      public TranslatedSeekableInput(long offset, long length, SeekableInput input) {
        this.length = length;
        this.delegate = input;
        this.offset = offset;
      }

      @Override
      public void seek(long p) throws IOException {
        delegate.seek(p + offset);
      }

      @Override
      public long tell() throws IOException {
        return delegate.tell() - offset;
      }

      @Override
      public long length() throws IOException {
        return length;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        return delegate.read(b, off, len);
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    }
  }
}
