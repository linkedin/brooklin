package com.linkedin.datastream.server.dms;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;


/**
 * BootstrapActionResources is the rest end point to process bootstrap datastream request
 */
@RestLiActions(name = "bootstrap", namespace = "com.linkedin.datastream.server.dms")
public class BootstrapActionResources {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapActionResources.class);

  private final DatastreamStore _store;
  private final Coordinator _coordinator;
  private final DatastreamServer _datastreamServer;
  private final ErrorLogger _errorLogger;

  private static final Counter CREATE_CALL = new Counter();
  private static final Counter CALL_ERROR = new Counter();
  private static final Histogram CREATE_CALL_LATENCY = new Histogram(new ExponentiallyDecayingReservoir());

  public BootstrapActionResources(DatastreamServer datastreamServer) {
    _datastreamServer = datastreamServer;
    _store = datastreamServer.getDatastreamStore();
    _coordinator = datastreamServer.getCoordinator();
    _errorLogger = new ErrorLogger(LOG);
  }

  /**
   * Process the request of creating bootstrap datastream. The request provides the bootstrap datastream
   * that needs to be created. Server will either return an existing datastream or create a new bootstrap datastream
   * based on the request.
   */
  @Action(name = "create")
  public Datastream create(@ActionParam("boostrapDatastream") Datastream bootstrapDatastream) {

    LOG.info(String.format("Create bootstrap datastream called with datastream %s", bootstrapDatastream));
    CREATE_CALL.inc();

    if (!bootstrapDatastream.hasName()) {
      CALL_ERROR.inc();
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Must specify name of Datastream!");
    }

    if (!bootstrapDatastream.hasConnectorType()) {
      CALL_ERROR.inc();
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Must specify connectorType!");
    }
    if (!bootstrapDatastream.hasSource()) {
      CALL_ERROR.inc();
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Must specify source of Datastream!");
    }

    long startTime = System.currentTimeMillis();

    try {
      _coordinator.initializeDatastream(bootstrapDatastream);
      _store.createDatastream(bootstrapDatastream.getName(), bootstrapDatastream);
    } catch (DatastreamAlreadyExistsException e) {
      CALL_ERROR.inc();
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_409_CONFLICT,
          "Failed to create bootstrap datastream: " + bootstrapDatastream, e);
    } catch (Exception e) {
      CALL_ERROR.inc();
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          String.format("Failed to initialize bootstrap Datastream %s", bootstrapDatastream), e);
    }

    CREATE_CALL_LATENCY.update(System.currentTimeMillis() - startTime);

    return bootstrapDatastream;
  }

  public static Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>();

    metrics.put(MetricRegistry.name(BootstrapActionResources.class, "createCall"), CREATE_CALL);
    metrics.put(MetricRegistry.name(BootstrapActionResources.class, "callError"), CALL_ERROR);
    metrics.put(MetricRegistry.name(BootstrapActionResources.class, "createCallLatency"), CREATE_CALL_LATENCY);

    return Collections.unmodifiableMap(metrics);
  }

}
