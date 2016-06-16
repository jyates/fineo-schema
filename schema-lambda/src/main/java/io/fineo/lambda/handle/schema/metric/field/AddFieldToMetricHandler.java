package io.fineo.lambda.handle.schema.metric.field;

import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A lambda handler that handles Kinesis events
 */
public abstract class AddFieldToMetricHandler extends RequestHandler<AddFieldToMetricRequest, AddFieldToMetricResponse> {

  private final Provider<FirehoseBatchWriter> archive;
  private final Provider<FirehoseBatchWriter> processErrors;
  private final Provider<FirehoseBatchWriter> commitFailures;
  private boolean hasProcessingError;

  public AddFieldToMetricHandler(Provider<FirehoseBatchWriter> archive,
    Provider<FirehoseBatchWriter> processErrors,
    Provider<FirehoseBatchWriter> commitFailures) {
    this.archive = archive;
    this.processErrors = processErrors;
    this.commitFailures = commitFailures;
  }

  @Override
  public void handle(KinesisEvent event) throws IOException {
    LOG.trace("Entering handler");
    int count = 0;
    for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
      count++;
      try {
        ByteBuffer data = rec.getKinesis().getData();
        data.mark();
        archive.get().addToBatch(data);
        data.reset();
        handleEvent(rec);
      } catch (RuntimeException e) {
        LOG.error("Failed for process record", e);
        addRecordError(rec);
      }
    }
    LOG.info("Handled " + count + " kinesis records");

    flushErrors();

    archive.get().flush();

    MultiWriteFailures<GenericRecord> failures = commit();
    LOG.debug("Finished writing record batches");
    FailureHandler.handle(failures, this.commitFailures::get);
  }

  protected abstract void handleEvent(KinesisEvent.KinesisEventRecord rec) throws IOException;

  protected abstract MultiWriteFailures<GenericRecord> commit() throws IOException;

  private void addRecordError(KinesisEvent.KinesisEventRecord rec) {
    ByteBuffer buff = rec.getKinesis().getData();
    buff.reset();
    processErrors.get().addToBatch(rec.getKinesis().getData());
    this.hasProcessingError = true;
  }

  private void flushErrors() throws IOException {
    if (!hasProcessingError) {
      LOG.debug("No error records found!");
      return;
    }

    LOG.trace("Flushing malformed records");
    processErrors.get().flush();
    LOG.debug("Flushed malformed records");
  }
}
