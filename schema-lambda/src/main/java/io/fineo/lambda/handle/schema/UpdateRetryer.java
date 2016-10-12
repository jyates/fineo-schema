package io.fineo.lambda.handle.schema;

import com.amazonaws.services.lambda.runtime.Context;
import io.fineo.lambda.handle.external.ExternalErrorsUtil;
import io.fineo.schema.OldSchemaException;
import io.fineo.schema.exception.SchemaNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run the attempts to update the schema a configurable number of times. Ignores
 * {@link OldSchemaException} and attempts to re-run because the update may have nothing to do
 * with the old schema. {@link SchemaNotFoundException} also gets retried to avoid eventual
 * consistency problems, but if never found will throw a 404 error.
 */
public class UpdateRetryer {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateRetryer.class);
  private int retries;

  public void setRetries(int retries) {
    this.retries = retries;
  }

  public <T> T run(ThrowingSupplier<T> run, Context context) throws Exception {
    OldSchemaException ex = null;
    for (int i = 0; i < retries; i++) {
      try {
        return run.doGet();
      } catch (OldSchemaException e) {
        // exception that is a problem internally
        done(e, i);
        ex = e;
      } catch (SchemaNotFoundException e2) {
        // exception that means the customer messed up, but we retry to ensure we cover an
        // eventual consistency.
        if (done(e2, i)) {
          throw ExternalErrorsUtil.get40X(context, 4, e2.getMessage());
        }
      }
    }
    throw ex;
  }

  private boolean done(Exception cause, int count) {
    boolean done = count == retries - 1;
    LOG.info("Failed to update schema because: {}\n" +
             (done ? "Giving up!" : "Trying again!"), cause);
    return done;
  }
}
