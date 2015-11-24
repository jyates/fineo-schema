package io.fineo.schema.aws.dynamodb;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.handlers.RequestHandler2;
import javafx.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class FaultInjectionHandler extends RequestHandler2 {
  private static final Log LOG = LogFactory.getLog(FaultInjectionHandler.class);
  private List<Pair<Class<? extends AmazonWebServiceRequest>, CountDownLatch>> slow =
    new ArrayList<>();
  private List<Pair<Class<? extends AmazonWebServiceRequest>, CountDownLatch>> tracking =
    new ArrayList<>();

  @Override
  public void beforeRequest(Request<?> request) {
    for (Pair<Class<? extends AmazonWebServiceRequest>, CountDownLatch> track : tracking) {
      if (request.getOriginalRequest().getClass().equals(track.getKey())) {
        track.getValue().countDown();
        LOG.info("Counted down tracking for: "+track.getKey());
      }
    }

    try {
      for (Pair<Class<? extends AmazonWebServiceRequest>, CountDownLatch> slowed : slow) {
        if (request.getOriginalRequest().getClass().equals(slowed.getKey())) {
          LOG.info("Waiting for slow latch for: "+slowed.getKey());
          slowed.getValue().await();
          LOG.info(" -- latch released -- ");
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void afterResponse(Request<?> request, Response<?> response) {

  }

  @Override
  public void afterError(Request<?> request, Response<?> response, Exception e) {

  }

  public void addSlowDown(Class<? extends AmazonWebServiceRequest> updateItemRequestClass,
    CountDownLatch latch) {
    this.slow.add(new Pair<>(updateItemRequestClass, latch));
  }

  public void addTracking(Class<? extends AmazonWebServiceRequest> updateItemRequestClass,
    CountDownLatch latch) {
    this.tracking.add(new Pair<>(updateItemRequestClass, latch));
  }
}
