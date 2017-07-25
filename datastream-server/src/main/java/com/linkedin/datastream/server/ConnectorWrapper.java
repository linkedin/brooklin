package com.linkedin.datastream.server;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * ConnectorWrapper wraps the Connector interface. It is a utility class used by the Coordinator.
 * The Coordinator should call the Connector API methods through this wrapper, which centralize
 * some bookkeeping features like logging and try-catch error handling.
 */
public class ConnectorWrapper {
  private final Logger _log;
  private final String _connectorType;

  private String _instanceName;
  private Connector _connector;
  private String _lastError;

  private long _startTime;
  private long _endTime;

  private AtomicLong _numDatastreams;
  private AtomicLong _numDatastreamTasks;

  public ConnectorWrapper(String connectorType, Connector connector) {
    _log = LoggerFactory.getLogger(String.format("%s:%s", ConnectorWrapper.class.getName(), connectorType));
    _connectorType = connectorType;
    _connector = connector;
    _numDatastreams = new AtomicLong(0);
    _numDatastreamTasks = new AtomicLong(0);
  }

  public boolean hasError() {
    return _lastError != null;
  }

  public String getLastError() {
    return _lastError;
  }

  public void setInstanceName(String instanceName) {
    _instanceName = instanceName;
  }

  private void logErrorAndException(String method, Exception ex) {
    String msg = "Failed to call connector API: Connector::" + method;
    _log.error(msg, ex);
    _lastError = msg + "\n" + ex.getMessage() + "\n" + ex.getStackTrace().toString();
  }

  private void logApiStart(String method) {
    _log.info(String.format("START: Connector::%s. Connector: %s, Instance: %s", method, _connectorType, _instanceName));
    _startTime = System.currentTimeMillis();
    _lastError = null;
  }

  private void logApiEnd(String method) {
    _endTime = System.currentTimeMillis();
    _log.info(String.format("END: Connector::%s. Connector: %s, Instance: %s, Duration: %d milliseconds", method,
        _connectorType, _instanceName, _endTime - _startTime));
  }

  public void start() {
    logApiStart("start");

    try {
      _connector.start();
    } catch (Exception ex) {
      logErrorAndException("start", ex);
      throw ex;
    }

    logApiEnd("start");
  }

  public void stop() {
    logApiStart("stop");

    try {
      _connector.stop();
    } catch (Exception ex) {
      logErrorAndException("stop", ex);
      throw ex;
    }

    logApiEnd("stop");
  }

  public Connector  getConnectorInstance() {
    return _connector;
  }

  public String getConnectorType() {
    return _connectorType;
  }

  public void onAssignmentChange(List<DatastreamTask> tasks) {
    logApiStart("onAssignmentChange");

    _numDatastreamTasks.set(tasks.size());
    _numDatastreams.set(tasks.stream().map(DatastreamTask::getDatastreams).flatMap(List::stream).distinct().count());

    try {
      _connector.onAssignmentChange(tasks);
    } catch (Exception ex) {
      logErrorAndException("onAssignmentChange", ex);
      throw ex;
    }

    logApiEnd("onAssignmentChange");
  }

  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    logApiStart("initializeDatastream");

    try {
      if (!stream.hasDestination()) {
        stream.setDestination(new DatastreamDestination());
      }
      _connector.initializeDatastream(stream, allDatastreams);
    } catch (Exception ex) {
      logErrorAndException("initializeDatastream", ex);
      throw ex;
    }

    logApiEnd("initializeDatastream");
  }

  public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    logApiStart("validateUpdateDatastreams");
    _connector.validateUpdateDatastreams(datastreams, allDatastreams);
    logApiEnd("validateUpdateDatastreams");
  }

  public long getNumDatastreams() {
    return _numDatastreams.get();
  }

  public long getNumDatastreamTasks() {
    return _numDatastreamTasks.get();
  }
}
