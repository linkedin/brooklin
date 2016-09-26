package com.linkedin.datastream.server.dms;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestLiActions;


/**
 * BootstrapActionResources is the rest end point to process bootstrap datastream request
 */
@RestLiActions(name = "bootstrap", namespace = "com.linkedin.datastream.server.dms")
public class BootstrapActionResources {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapActionResources.class);
  private static final String CLASS_NAME = BootstrapActionResources.class.getSimpleName();

  private final DatastreamStore _store;
  private final Coordinator _coordinator;
  private final DatastreamServer _datastreamServer;
  private final ErrorLogger _errorLogger;

  private static final Meter CREATE_CALL = new Meter();
  private static final Meter CALL_ERROR = new Meter();
  private static AtomicLong _createCallLatencyMs = new AtomicLong(0L);
  private static final Gauge<Long> CREATE_CALL_LATENCY_MS = () -> _createCallLatencyMs.get();

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
    try {
      LOG.info(String.format("Create bootstrap datastream called with datastream %s", bootstrapDatastream));
      CREATE_CALL.mark();

      if (!bootstrapDatastream.hasName()) {
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
            "Must specify name of Datastream!");
      }

      if (!bootstrapDatastream.hasConnectorName()) {
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Must specify connectorType!");
      }
      if (!bootstrapDatastream.hasSource()) {
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
            "Must specify source of Datastream!");
      }

      Instant startTime = Instant.now();

      try {
        _coordinator.initializeDatastream(bootstrapDatastream);
        _store.createDatastream(bootstrapDatastream.getName(), bootstrapDatastream);
      } catch (DatastreamAlreadyExistsException e) {
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_409_CONFLICT,
            "Failed to create bootstrap datastream: " + bootstrapDatastream, e);
      }

      _createCallLatencyMs.set(Duration.between(startTime, Instant.now()).toMillis());

      return bootstrapDatastream;
    } catch (RestLiServiceException e) {
      CALL_ERROR.mark();
      throw e;
    } catch (Exception e) {
      CALL_ERROR.mark();
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          String.format("Failed to initialize bootstrap Datastream %s", bootstrapDatastream), e);
    }

    return null;
  }

  public static Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>();

    metrics.put(MetricRegistry.name(CLASS_NAME, "createCall"), CREATE_CALL);
    metrics.put(MetricRegistry.name(CLASS_NAME, "callError"), CALL_ERROR);
    metrics.put(MetricRegistry.name(CLASS_NAME, "createCallLatencyMs"), CREATE_CALL_LATENCY_MS);

    return Collections.unmodifiableMap(metrics);
  }
}
