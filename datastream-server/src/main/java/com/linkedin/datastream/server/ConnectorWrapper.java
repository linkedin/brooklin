/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * ConnectorWrapper wraps the Connector interface. It is a utility class used by the Coordinator.
 * The Coordinator should call the Connector API methods through this wrapper, which centralize
 * some bookkeeping features like logging and try-catch error handling.
 */
public class ConnectorWrapper {
  private final Logger _log;
  private final String _connectorType;

  private String _instanceName;
  private final Connector _connector;
  private String _lastError;

  private long _startTime;
  private long _endTime;

  private final AtomicLong _numDatastreams;
  private final AtomicLong _numDatastreamTasks;

  /**
   * Create ConnectorWrapper that wraps the provided Connector
   * @param connectorType Name of the connector
   * @param connector Connector for which wrapper should be created.
   */
  public ConnectorWrapper(String connectorType, Connector connector) {
    _log = LoggerFactory.getLogger(String.format("%s:%s", ConnectorWrapper.class.getName(), connectorType));
    _connectorType = connectorType;
    _connector = connector;
    _numDatastreams = new AtomicLong(0);
    _numDatastreamTasks = new AtomicLong(0);
  }

  /**
   * Check if any error was encountered within the connector the last time a connector API was called
   * Note: The error resets every time an API is called.
   * @return true if any error was seen
   */
  public boolean hasError() {
    return _lastError != null;
  }

  /**
   * Get the message of the last error encountered the last time a connector API was called (if any)
   * Note: The error message resets every time an API is called.
   * @return Error message (if any), null otherwise.
   */
  public String getLastError() {
    return _lastError;
  }

  public void setInstanceName(String instanceName) {
    _instanceName = instanceName;
  }

  private void logErrorAndException(String method, Exception ex) {
    String msg = "Failed to call connector API: Connector::" + method;
    _log.error(msg, ex);
    _lastError = msg + "\n" + ex.getMessage() + "\n" + Arrays.toString(ex.getStackTrace());
  }

  private void logApiStart(String method) {
    _log.info("START: Connector::{}. Connector: {}, Instance: {}", method, _connectorType, _instanceName);
    _startTime = System.currentTimeMillis();
    _lastError = null;
  }

  private void logApiEnd(String method) {
    _endTime = System.currentTimeMillis();
    _log.info("END: Connector::{}. Connector: {}, Instance: {}, Duration: {} milliseconds", method,
        _connectorType, _instanceName, _endTime - _startTime);
  }

  /**
   * Wrapper API to start connector.
   * @param checkpointProvider CheckpointProvider if the connector needs a checkpoint store.
   */
  public void start(CheckpointProvider checkpointProvider) {
    logApiStart("start");

    try {
      _connector.start(checkpointProvider);
    } catch (Exception ex) {
      logErrorAndException("start", ex);
      throw ex;
    }

    logApiEnd("start");
  }

  /**
   * Wrapper API to stop connector.
   */
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

  public Connector getConnectorInstance() {
    return _connector;
  }

  public String getConnectorType() {
    return _connectorType;
  }

  /**
   * Wrapper API to notify connector about changes in DatastreamTask assignment.
   * @param tasks the list of currently assigned tasks
   */
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

  /**
   * Wrapper API to initialize datastream in connector. This API calls connector's {@link Connector#initializeDatastream}
   * @param stream Datastream to initialize.
   * @param allDatastreams all existing datastreams in the system that have the same connector type as the datastream
   *                       being initialized.
   * @throws DatastreamValidationException when the datastream that is being created fails any validation.
   */
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    logApiStart("initializeDatastream");

    try {
      if (!stream.hasDestination()) {
        stream.setDestination(new DatastreamDestination());
      }
      _connector.initializeDatastream(stream, allDatastreams);
    } catch (DatastreamValidationException ex) {
      throw ex;
    } catch (Exception ex) {
      logErrorAndException("initializeDatastream", ex);
      throw ex;
    }

    logApiEnd("initializeDatastream");
  }

  /**
   * Wrapper API to validate updates to given datastreams. This API calls connector's {@link Connector#validateUpdateDatastreams}.
   * @param datastreams List of datastreams whose update needs to be validated.
   * @param allDatastreams all existing datastreams in the system that have the same connector type as the datastream
   *    *                  being updated.
   * @throws DatastreamValidationException
   */
  public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    logApiStart("validateUpdateDatastreams");
    _connector.validateUpdateDatastreams(datastreams, allDatastreams);
    logApiEnd("validateUpdateDatastreams");
  }

  /**
   * Wrapper API to decide if give update operation type is supported for given datastream. This API in turn calls
   * connector's isDatastreamUpdateTypeSupported API.
   * @param datastream Datastream on which update operation will be performed.
   * @param updateType Type of datastream update which needs to be checked against.
   * @return true if given operation type is supported, false otherwise.
   */
  public boolean isDatastreamUpdateTypeSupported(Datastream datastream, DatastreamConstants.UpdateType updateType) {
    logApiStart("isDatastreamUpdateTypeSupported");
    boolean ret = _connector.isDatastreamUpdateTypeSupported(datastream, updateType);
    logApiEnd("isDatastreamUpdateTypeSupported");
    return ret;
  }

  public long getNumDatastreams() {
    return _numDatastreams.get();
  }

  public long getNumDatastreamTasks() {
    return _numDatastreamTasks.get();
  }

  /**
   * Wrapper API to find out destination of given datastream. This API calls connector's {@link Connector#getDestinationName}.
   * @param datastream Datastream whose destination needs to be found out.
   * @return Destination of the datastream.
   */
  public String getDestinationName(Datastream datastream) {
    logApiStart("getDestinationName");
    String destinationName = _connector.getDestinationName(datastream);
    logApiEnd("getDestinationName");
    return destinationName;
  }

  /**
   * Hook that can be used to do any final connector related initializations on datastream, after
   * datastream initialization is done at connector, transport, and coordinator level.
   * @param stream Datastream being initialized
   * @param allDatastreams all existing datastreams in the system of connector type of the datastream that is being
   *                       initialized.
   */
  public void postDatastreamInitialize(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    logApiStart("postDatastreamInitialize");

    try {
      _connector.postDatastreamInitialize(stream, allDatastreams);
    } catch (DatastreamValidationException ex) {
      throw ex;
    } catch (Exception ex) {
      logErrorAndException("postDatastreamInitialize", ex);
      throw ex;
    }

    logApiEnd("postDatastreamInitialize");
  }
}
