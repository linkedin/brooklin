package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * ConnectorWrapper wraps the Connector interface. It is a utility class used by the Coordinator.
 * The Coordinator should call the Connector API methods through this wrapper, which centralize
 * some bookkeeping features like logging and try-catch error handling.
 */
public class ConnectorWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectorWrapper.class.getName());

  private String _instanceName;
  private Connector _connector;
  private String _lastError;

  private long _startTime;
  private long _endTime;

  public ConnectorWrapper(Connector connector) {
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
    LOG.info(String.format("START: Connector::%s. Connector: %s, Instance: %s", method, _connector.getConnectorType(),
        _instanceName));
    _startTime = System.currentTimeMillis();
    _lastError = null;
  }

  private void logApiEnd(String method) {
    _endTime = System.currentTimeMillis();
    LOG.info(String.format("END: Connector::%s. Connector: %s, Instance: %s, Duration: %d milliseconds", method,
        _connector.getConnectorType(), _instanceName, _endTime - _startTime));
  }

  public void start(DatastreamEventCollectorFactory collectorFactory) {
    logApiStart("start");

    try {
      _connector.start(collectorFactory);
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
    logApiStart("getConnectorType");
    String ret = null;

    try {
      ret = _connector.getConnectorType();
    } catch (Exception ex) {
      logErrorAndException("getConnectorType", ex);
    }

    logApiEnd("getConnectorType");
    return ret;
  }

  public void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {
    logApiStart("onAssignmentChange");

    try {
      _connector.onAssignmentChange(context, tasks);
    } catch (Exception ex) {
      logErrorAndException("onAssignmentChange", ex);
    }

    logApiEnd("onAssignmentChange");
  }

  public DatastreamTarget getDatastreamTarget(Datastream stream) {
    logApiStart("getDatastreamTarget");

    DatastreamTarget ret = null;

    try {
      ret = _connector.getDatastreamTarget(stream);
    } catch (Exception ex) {
      logErrorAndException("getDatastreamTarget", ex);
    }

    logApiEnd("getDatastreamTarget");

    return ret;
  }

  public DatastreamValidationResult validateDatastream(Datastream stream) {
    logApiStart("validateDatastream");
    DatastreamValidationResult ret = null;

    try {
      ret = _connector.validateDatastream(stream);
    } catch (Exception ex) {
      logErrorAndException("alidateDatastream", ex);
    }

    logApiEnd("validateDatastream");

    return ret;
  }
}
