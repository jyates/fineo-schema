package io.fineo.lambda.handle.schema;

import io.fineo.schema.OldSchemaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class UpdateRetryer {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateRetryer.class);
  private int retries;

  public void setRetries(int retries) {
    this.retries = retries;
  }

  public <T> T run(ThrowingSupplier<T> run) throws Exception{
    OldSchemaException ex = null;
    for (int i = 0; i < retries; i++) {
      try {
        return run.doGet();
      } catch (OldSchemaException e) {
        LOG.info("Failed to update schema because: {}\n" +
                 ((i == retries - 1) ? "Giving up!" : "Trying again!"), e);
        ex = e;
      }
    }
    throw ex;
  }
}
