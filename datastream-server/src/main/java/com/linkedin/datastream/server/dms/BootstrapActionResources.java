package com.linkedin.datastream.server.dms;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
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

  private static final String CREATE_CALL = "createCall";
  private static final String CALL_ERROR = "callError";
  private static AtomicLong _createCallLatencyMs = new AtomicLong(0L);
  private static final Gauge<Long> CREATE_CALL_LATENCY_MS = () -> _createCallLatencyMs.get();
  private static final String CREATE_CALL_LATENCY_MS_STRING = "createCallLatencyMs";

  private final DynamicMetricsManager _dynamicMetricsManager;

  public BootstrapActionResources(DatastreamServer datastreamServer) {
    _datastreamServer = datastreamServer;
    _store = datastreamServer.getDatastreamStore();
    _coordinator = datastreamServer.getCoordinator();
    _errorLogger = new ErrorLogger(LOG);

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _dynamicMetricsManager.registerMetric(this.getClass(), CREATE_CALL_LATENCY_MS_STRING, CREATE_CALL_LATENCY_MS);
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
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), CREATE_CALL, 1);

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
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), CALL_ERROR, 1);
      throw e;
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          String.format("Failed to initialize bootstrap Datastream %s", bootstrapDatastream), e);
    }

    return null;
  }

  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();

    metrics.add(new BrooklinMeterInfo(MetricRegistry.name(CLASS_NAME, CREATE_CALL)));
    metrics.add(new BrooklinMeterInfo(MetricRegistry.name(CLASS_NAME, CALL_ERROR)));
    metrics.add(new BrooklinGaugeInfo(MetricRegistry.name(CLASS_NAME, CREATE_CALL_LATENCY_MS_STRING)));

    return Collections.unmodifiableList(metrics);
  }
}
