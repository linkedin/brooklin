package com.linkedin.datastream.server;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * ConnectorWrapper wraps the Connector interface. It is a utility class used by the Coordinator.
 * The Coordinator should call the Connector API methods through this wrapper, which centralize
 * some bookkeeping features like logging and try-catch error handling.
 */
public class ConnectorWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectorWrapper.class.getName());
  private final String _connectorType;

  private String _instanceName;
  private Connector _connector;
  private String _lastError;

  private long _startTime;
  private long _endTime;

  public ConnectorWrapper(String connectorType, Connector connector) {
    _connectorType = connectorType;
    _connector = connector;
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
    LOG.error(msg, ex);
    _lastError = msg + "\n" + ex.getMessage() + "\n" + ex.getStackTrace().toString();
  }

  private void logApiStart(String method) {
    LOG.info(String.format("START: Connector::%s. Connector: %s, Instance: %s", method, _connectorType, _instanceName));
    _startTime = System.currentTimeMillis();
    _lastError = null;
  }

  private void logApiEnd(String method) {
    _endTime = System.currentTimeMillis();
    LOG.info(String.format("END: Connector::%s. Connector: %s, Instance: %s, Duration: %d milliseconds", method,
        _connectorType, _instanceName, _endTime - _startTime));
  }

  public void start() {
    logApiStart("start");

    try {
      _connector.start();
    } catch (Exception ex) {
      logErrorAndException("start", ex);
    }

    logApiEnd("start");
  }

  public void stop() {
    logApiStart("stop");

    try {
      _connector.stop();
    } catch (Exception ex) {
      logErrorAndException("stop", ex);
    }

    logApiEnd("stop");
  }

  public String getConnectorType() {
    return _connectorType;
  }

  public void onAssignmentChange(List<DatastreamTask> tasks) {
    logApiStart("onAssignmentChange");

    try {
      _connector.onAssignmentChange(tasks);
    } catch (Exception ex) {
      logErrorAndException("onAssignmentChange", ex);
    }

    logApiEnd("onAssignmentChange");
  }

  public void initializeDatastream(Datastream stream) throws DatastreamValidationException {
    logApiStart("initializeDatastream");

    try {
      _connector.initializeDatastream(stream);
    } catch (Exception ex) {
      logErrorAndException("initializeDatastream", ex);
      throw ex;
    }

    logApiEnd("initializeDatastream");
  }
}
