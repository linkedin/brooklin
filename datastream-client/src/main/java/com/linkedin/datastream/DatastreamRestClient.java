/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamNotFoundException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.RetriesExhaustedException;
import com.linkedin.datastream.server.dms.DatastreamRequestBuilders;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.ActionRequest;
import com.linkedin.restli.client.BatchUpdateRequest;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.DeleteRequest;
import com.linkedin.restli.client.FindRequest;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.client.response.BatchKVResponse;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.IdResponse;
import com.linkedin.restli.common.UpdateStatus;


/**
 * Datastream REST Client
 */
public class DatastreamRestClient {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamRestClient.class);
  @VisibleForTesting
  protected static final String DATASTREAM_UUID = "datastreamUUID";

  // To support retries on the request timeouts
  public static final String CONFIG_RETRY_PERIOD_MS = "retryPeriodMs";
  public static final String CONFIG_RETRY_TIMEOUT_MS = "retryTimeoutMs";
  private static final long DEFAULT_RETRY_PERIOD_MS = Duration.ofSeconds(6).toMillis();
  private static final long DEFAULT_RETRY_TIMEOUT_MS = Duration.ofSeconds(90).toMillis();

  private final DatastreamRequestBuilders _builders;
  private final RestClient _restClient;

  private long _retryPeriodMs = DEFAULT_RETRY_PERIOD_MS;
  private long _retryTimeoutMs = DEFAULT_RETRY_TIMEOUT_MS;

  /**
   * Construct the DatastreamRestClient
   * @deprecated Please use {@link DatastreamRestClientFactory} instead
   * @param restClient pre-created RestClient
   */
  @Deprecated
  public DatastreamRestClient(RestClient restClient) {
    this(restClient, new Properties());
  }

  /**
   * Construct the DatastreamRestClient. Should be called by {@link DatastreamRestClientFactory} only
   * @param restClient rest.li client to use
   * @param config config for the DatastreamRestClient. Note that this is not the HTTP config for the underlying
   *               RestClient
   */
  public DatastreamRestClient(RestClient restClient, Properties config) {
    if (config.containsKey(CONFIG_RETRY_PERIOD_MS)) {
      _retryPeriodMs = Long.valueOf(config.getProperty(CONFIG_RETRY_PERIOD_MS));
    }
    if (config.containsKey(CONFIG_RETRY_TIMEOUT_MS)) {
      _retryTimeoutMs = Long.valueOf(config.getProperty(CONFIG_RETRY_TIMEOUT_MS));
    }
    Validate.isTrue(_retryPeriodMs > 0);
    Validate.isTrue(_retryTimeoutMs > _retryPeriodMs);
    Validate.notNull(restClient, "null restClient");
    _builders = new DatastreamRequestBuilders();
    _restClient = restClient;
    LOG.info("DatastreamRestClient created with retryPeriodMs={} retryTimeoutMs={}", _retryPeriodMs, _retryTimeoutMs);
  }

  private long getRetryPeriodMs() {
    // give a bit of randomness to the retry period; in the mean time, retry period can't exceed retry timeout
    return Math.min(Math.round(_retryPeriodMs * RandomUtils.nextDouble(0.6, 1.4)) + 1, _retryTimeoutMs);
  }

  private long getRetryTimeoutMs() {
    return _retryTimeoutMs;
  }

  private Datastream doGetDatastream(String datastreamName) throws RemoteInvocationException {
    GetRequest<Datastream> request = _builders.get().id(datastreamName).build();
    ResponseFuture<Datastream> datastreamResponseFuture = _restClient.sendRequest(request);
    return datastreamResponseFuture.getResponseEntity();
  }

  private static boolean isNotFoundHttpStatus(RemoteInvocationException e) {
    return (e instanceof RestLiResponseException
        && ((RestLiResponseException) e).getStatus() == HttpStatus.S_404_NOT_FOUND.getCode());
  }

  /**
   * Get the complete datastream object corresponding to the datastream name. This method makes a GET REST call
   * to the Datastream management service which in turn fetches this Datastream object from the store (ZooKeeper).
   * @param datastreamName
   *    Name of the datastream that should be retrieved.
   * @return
   *    Datastream object corresponding to the datastream. This method will not return null.
   * @throws com.linkedin.datastream.common.DatastreamRuntimeException
   *    Throws DatastreamNotFoundException if the datastream doesn't exist,
   *    Throws DatastreamRuntimeException for any other errors encountered while fetching the datastream.
   *    If there are any other network/system level errors while sending the request or receiving the response.
   */
  public Datastream getDatastream(String datastreamName) {
    Instant startTime = Instant.now();
    Datastream datastream = PollUtils.poll(() -> {
      try {
        return doGetDatastream(datastreamName);
      } catch (RemoteInvocationException e) {
        // instanceof works for null as well
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: getDatastream. May retry...", e);
          return null;
        }
        if (isNotFoundHttpStatus(e)) {
          LOG.warn(String.format("Datastream {%s} is not found", datastreamName), e);
          throw new DatastreamNotFoundException(datastreamName, e);
        } else {
          String errorMessage = String.format("Get Datastream {%s} failed with error.", datastreamName);
          ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
          return null; // not reachable; Meltdown hack goes here...
        }
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
    LOG.info("getDatastream for datastream {} took {} ms", Duration.between(startTime, Instant.now()).toMillis());
    return datastream;
  }

  /**
   * After creating the datastream, initialization of the datastream is an async process.
   * Initialization typically involves creating the destination topic, creating the datastream tasks and
   * assigning them to the datastream instances for producing.
   * @param datastreamName
   *   Name of the datastream
   * @param timeoutMs
   *   wait timeout in milliseconds
   * @return
   *   Returns the initialized datastream object.
   */
  public Datastream waitTillDatastreamIsInitialized(String datastreamName, long timeoutMs) throws InterruptedException {
    final int pollIntervalMs = 500;
    final long startTimeMs = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Datastream ds = getDatastream(datastreamName);
      if (ds.hasStatus() && ds.getStatus() == DatastreamStatus.READY) {
        return ds;
      }
      Thread.sleep(pollIntervalMs);
    }

    String errorMessage = String.format("Datastream was not initialized before the timeout %s", timeoutMs);
    LOG.error(errorMessage);
    throw new DatastreamRuntimeException(errorMessage);
  }

  /**
   * DeleteDatastream just marks the datastream for deletion. Hard delete of the datastream is an async process.
   * This method waits till the datastream is completely removed from the system.
   * @param datastreamName Name of the datastream.
   * @param timeoutMs wait timeout in milliseconds.
   */
  public void waitTillDatastreamIsDeleted(String datastreamName, long timeoutMs) throws InterruptedException {
    final int pollIntervalMs = 500;
    final long startTimeMs = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      try {
        getDatastream(datastreamName);
      } catch (DatastreamNotFoundException e) {
        return;
      }
      Thread.sleep(pollIntervalMs);
    }

    String errorMessage = String.format("Datastream was not deleted before the timeout %s", timeoutMs);
    LOG.error(errorMessage);
    throw new DatastreamRuntimeException(errorMessage);
  }

  private List<Datastream> getAllDatastreams(GetAllRequest<Datastream> request) {
    Instant startTime = Instant.now();
    List<Datastream> datastreams = PollUtils.poll(() -> {
      ResponseFuture<CollectionResponse<Datastream>> datastreamResponseFuture = _restClient.sendRequest(request);
      try {
        return datastreamResponseFuture.getResponse().getEntity().getElements();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: getAllDatastreams. May retry...", e);
          return null;
        }
        String errorMessage = "Get All Datastreams failed with error.";
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        return null; // not reachable
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
    LOG.info("getAllDatastreams took {} ms", Duration.between(startTime, Instant.now()).toMillis());
    return datastreams;
  }

  /**
   * Get all the datastream objects available on the sever. This method makes a GET REST call
   * to the Datastream management service which in turn fetches all the Datastream objects from the store (ZooKeeper).
   * Entries will be returned in lexicographical order based on their getName() property.
   *
   * @return all the Datastream objects
   * @for any errors encountered while fetching the datastream.
   */
  public List<Datastream> getAllDatastreams() {
    return getAllDatastreams(_builders.getAll().build());
  }

  /**
   * Get all the datastream objects available on the sever. This method makes a GET REST call
   * to the Datastream management service which in turn fetches all the Datastream objects from the store (ZooKeeper).
   * Entries will be returned in lexicographical order based on their getName() property.
   *
   * @param start index of the first datastream to produce
   * @param count maximum number of entries to be produced
   */
  public List<Datastream> getAllDatastreams(int start, int count) {
    return getAllDatastreams(_builders.getAll().paginate(start, count).build());
  }

  /**
   * Creates a new datastream. Name of the datastream must be unique. This method makes a POST REST call to the
   * Datastream management service which validates the datastream object and writes it to the store (ZooKeeper).
   * @param datastream Datastream to create
   */
  public void createDatastream(Datastream datastream) {
    Instant startTime = Instant.now();
    String creationUid = UUID.randomUUID().toString();

    PollUtils.poll(() -> {
      if (!datastream.hasMetadata()) {
        datastream.setMetadata(new StringMap());
      }
      datastream.getMetadata().put(DATASTREAM_UUID, creationUid);
      CreateIdRequest<String, Datastream> request = _builders.create().input(datastream).build();
      ResponseFuture<IdResponse<String>> datastreamResponseFuture = _restClient.sendRequest(request);
      try {
        return datastreamResponseFuture.getResponse();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: createDatastream. May retry...", e);
          return null;
        }

        if (e instanceof RestLiResponseException) {
          int errorCode = ((RestLiResponseException) e).getStatus();
          if (errorCode == HttpStatus.S_409_CONFLICT.getCode()) {
            // Timeout on previous request can make it appear as though datastream already existed.
            // Check if the datastream was in fact created by this request.
            Datastream existingDatastream = getDatastream(datastream.getName());
            Optional<String> existingUid = Optional.ofNullable(existingDatastream.getMetadata().get(DATASTREAM_UUID));
            if (existingUid.isPresent() && existingUid.get().equals(creationUid)) {
              return existingDatastream;
            }

            String msg = String.format("Datastream %s exists with the same name. Requested datastream %s",
                existingDatastream.toString(), datastream.toString());
            LOG.warn(msg, e);
            throw new DatastreamAlreadyExistsException(msg);
          } else if (errorCode == HttpStatus.S_403_FORBIDDEN.getCode()) {
            // Handle any DMS REST authorization failure for the caller principal
            String msg = "Client is not authorized to invoke Datastream-CREATE, stream=" + datastream.getName();
            ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, msg, e);
          }
        }

        String errorMessage = String.format("Create Datastream %s failed with error.", datastream);
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        return null; // unreachable
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
    LOG.info("createDatastream for datastream {} took {} ms", datastream,
        Duration.between(startTime, Instant.now()).toMillis());
  }

  /**
   * Update a datastream. Validation will be performed on the server side to ensure certain conditions are met
   * (e.g. datastream is valid, the connector type supports datastream updates, etc.)
   * @param datastream datastream to be updated
   */
  public void updateDatastream(Datastream datastream) {
    updateDatastream(Collections.singletonList(datastream));
  }

  /**
   * Update datastreams in batch. Either all datastreams get updated or none get updated. Validation will be
   * performed on the server side to ensure certain conditions are met (e.g. datastreams are valid, the connector
   * type supports datastream updates, etc.)
   * @param datastreams list of datastreams to be updated
   */
  public void updateDatastream(List<Datastream> datastreams) {
    Instant startTime = Instant.now();
    // we won't support partial success. so ignore the result
    PollUtils.poll(() -> {
      BatchUpdateRequest<String, Datastream> request = _builders.batchUpdate()
          .inputs(datastreams.stream().collect(Collectors.toMap(Datastream::getName, ds -> ds)))
          .build();
      ResponseFuture<BatchKVResponse<String, UpdateStatus>> datastreamResponseFuture = _restClient.sendRequest(request);
      try {
        return datastreamResponseFuture.getResponse();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: updateDatastream. May retry...", e);
          return null;
        }
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, "Failed to update datastreams", e);
        return null; // not reachable
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);

    LOG.info("updateDatastreams for datastreams {} took {} ms", datastreams,
        Duration.between(startTime, Instant.now()).toMillis());
  }

  /**
   * Delete the datastream with the specified name. This method makes a DELETE REST call to the Datastream management service
   * on the DatastreamResource which in turn deletes the entity from ZooKeeper. All the connectors that
   * are serving the datastream will get notified to stop producing events for the datastream.
   * @param datastreamName
   *   Name of the datastream that should be deleted.
   * @throws DatastreamRuntimeException
   *   When the datastream is not found or any other error happens on the server.
   */
  public void deleteDatastream(String datastreamName) {
    Instant startTime = Instant.now();
    PollUtils.poll(() -> {
      DeleteRequest<Datastream> request = _builders.delete().id(datastreamName).build();
      ResponseFuture<EmptyRecord> response = _restClient.sendRequest(request);
      try {
        return response.getResponse();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: deleteDatastream. May retry...", e);
          return null;
        }
        String errorMessage = String.format("Delete Datastream %s failed with error.", datastreamName);
        if (isNotFoundHttpStatus(e)) {
          LOG.error(errorMessage, e);
          throw new DatastreamNotFoundException(datastreamName, e);
        } else {
          ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        }
        return null; // not reachable
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);

    LOG.info("deleteDatastream for datastream {} took {} ms", datastreamName,
        Duration.between(startTime, Instant.now()).toMillis());
  }

  /**
   * Check whether the specified datastream exists in the current Datastream cluster.
   * @param datastreamName name of the datastream to be checked
   * @return true if such datastream exists
   */
  public boolean datastreamExists(String datastreamName) {
    return PollUtils.poll(() -> {
      try {
        doGetDatastream(datastreamName);
        return Boolean.TRUE;
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: datastreamExists. May retry...", e);
          return null;
        }
        if (isNotFoundHttpStatus(e)) {
          LOG.debug("Datastream {} is not found", datastreamName);
        } else {
          String errorMessage = String.format("Get Datastream %s failed with error.", datastreamName);
          ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        }
      }
      return Boolean.FALSE;
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
  }

  /**
   * Pause a datastream, by changing its status to PAUSED. In case there are multiple datastreams
   * in a group, the group is PAUSED if ALL the datastreams are paused
   * @param datastreamName Name of the datastream to pause.
   * @throws DatastreamRuntimeException in case of a communication issue or an error response from
   * the server.
   */
  public void pause(String datastreamName) throws RemoteInvocationException {
    pause(datastreamName, false);
  }

  /**
   * Pause a datastream, by changing its status to PAUSED. In case there are multiple datastreams
   * in a group, the group is only PAUSED if ALL the datastreams are paused
   * @param datastreamName Name of the datastream to pause.
   * @param force pause all other datastreams within the same group
   * @throws DatastreamRuntimeException in case of a communication issue or an error response from
   * the server.
   */
  public void pause(String datastreamName, boolean force) throws RemoteInvocationException {
    pauseOrStop(datastreamName, force, true);
  }

  /**
   * Stop a datastream, by changing its status to Stopped. This is similar to pause but it is treated as
   * unassigned and it won't generate any metric
   * @param datastreamName Name of the datastream to stop.
   * @throws DatastreamRuntimeException in case of a communication issue or an error response from
   * the server.
   */
  public void stop(String datastreamName) throws RemoteInvocationException {
    pauseOrStop(datastreamName, false, false);
  }


  /**
   * Pause/Stop a datastream, by changing its status to PAUSED/STOPPED
   * @param datastreamName
   *    Name of the datastream to pause.
   * @param force
   *    If true, change all the datastreams in the same group to PAUSED/STOPPED
   * @throws DatastreamRuntimeException in case of a communication issue or an error response from
   * the server.
   */
  private void pauseOrStop(String datastreamName, boolean force, boolean isPaused) {
    Instant startTime = Instant.now();
    String action = isPaused ? "PAUSE" : "STOP";
    PollUtils.poll(() -> {
      try {
        ActionRequest<Void> request = isPaused ? _builders.actionPause().id(datastreamName).forceParam(force).build()
            : _builders.actionStop().id(datastreamName).forceParam(force).build();
        ResponseFuture<Void> datastreamResponseFuture = _restClient.sendRequest(request);
        return datastreamResponseFuture.getResponse();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: " + action + ". May retry...", e);
          return null;
        }
        if (isNotFoundHttpStatus(e)) {
          LOG.warn(String.format("Datastream {%s} is not found", datastreamName), e);
          throw new DatastreamNotFoundException(datastreamName, e);
        } else {
          String errorMessage = String.format("Pause Datastream {%s} failed with error.", datastreamName);
          ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        }
        return null; // not reachable
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
    LOG.info(action + " for datastream {} took {} ms", datastreamName,
        Duration.between(startTime, Instant.now()).toMillis());
  }

  /**
   * Resume a datastream, by changing its status from PAUSED to READY.
   * @param datastreamName
   *    Name of the datastream to resume.
   * @throws DatastreamRuntimeException in case of a communication issue or
   * an error response from the server.
   */
  public void resume(String datastreamName) throws RemoteInvocationException {
    resume(datastreamName, false);
  }

  /**
   * Resume a datastream, by changing its status from PAUSED to READY.
   * @param datastreamName
   *    Name of the datastream to resume.
   * @param force
   *    If true, changes all the datastreams in the same group to READY.
   * @throws DatastreamRuntimeException in case of a communication issue or
   * an error response from the server.
   */
  public void resume(String datastreamName, boolean force) throws RemoteInvocationException {
    Instant startTime = Instant.now();
    PollUtils.poll(() -> {
      try {
        ActionRequest<Void> request = _builders.actionResume().id(datastreamName).forceParam(force).build();
        ResponseFuture<Void> datastreamResponseFuture = _restClient.sendRequest(request);
        return datastreamResponseFuture.getResponse();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: pause. May retry...", e);
          return null;
        }
        if (isNotFoundHttpStatus(e)) {
          LOG.warn(String.format("Datastream {%s} is not found", datastreamName), e);
          throw new DatastreamNotFoundException(datastreamName, e);
        } else {
          String errorMessage = String.format("Resume Datastream {%s} failed with error.", datastreamName);
          ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        }
        return null; // not reachable
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
    LOG.info("resume for datastream {} took {} ms", datastreamName,
        Duration.between(startTime, Instant.now()).toMillis());
  }

  /**
   * Get all the datastream objects that are in the same group as the datastream whose name is "datastreamName".
   * This method makes a GET REST call to the Datastream management service which in turn fetches all the Datastream
   * objects from the store (ZooKeeper). Entries will be return in lexicographical based on their getName() property.
   *
   * @return all the Datastream objects that are in the same group as the supplied datastreamName
   * @throws DatastreamRuntimeException in case of a communication issue or an error response from the server.
   */
  public List<Datastream> findGroup(String datastreamName) {
    return PollUtils.poll(() -> {
      try {
        FindRequest<Datastream> request = _builders.findByFindGroup().datastreamNameParam(datastreamName).build();
        return _restClient.sendRequest(request).getResponse().getEntity().getElements();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: findGroup. May retry...", e);
          return null;
        }
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, "findGroup failed with error.", e);
      }
      return null; // not reachable
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
  }

  /**
   * Pause source partitions for a particular datastream.
   * @param datastreamName
   *    Name of the datastream to be paused.
   * @param sourcePartitions
   *    StringMap of format <source, comma separated list of partitions or "*">. Example: <"FooTopic", "0,13,2">
   *                         or <"FooTopic","*">
   * @throws DatastreamRuntimeException in case of a communication issue or an error response from the server.
   */
  public void pauseSourcePartitions(String datastreamName, StringMap sourcePartitions)
      throws RemoteInvocationException {
    Instant startTime = Instant.now();
    PollUtils.poll(() -> {
      try {
        ActionRequest<Void> request =
            _builders.actionPauseSourcePartitions().id(datastreamName).sourcePartitionsParam(sourcePartitions).build();
        ResponseFuture<Void> datastreamResponseFuture = _restClient.sendRequest(request);
        return datastreamResponseFuture.getResponse();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: pauseSourcePartitions. May retry...", e);
          return null;
        }
        if (isNotFoundHttpStatus(e)) {
          LOG.warn(String.format("Datastream {%s} is not found", datastreamName), e);
          throw new DatastreamNotFoundException(datastreamName, e);
        } else {
          String errorMessage =
              String.format("Pause Datastream partitions failed with error. Datastream: {%s}, Partitions: {%s}",
                  datastreamName, sourcePartitions);
          ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        }
        return null; // not reachable
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
    LOG.info("pauseSourcePartitions for datastream {}, sourcePartitions {} took {} ms", datastreamName, sourcePartitions,
        Duration.between(startTime, Instant.now()).toMillis());
  }

  /**
   * Resume source partitions for a particular datastream.
   * @param datastreamName
   *    Name of the datastream to be paused.
   * @param sourcePartitions
   *    StringMap of format <source, comma separated list of partitions or "*">. Example: <"FooTopic", "0,13,2">
   *                         or <"FooTopic","*">
   * @throws DatastreamRuntimeException in case of a communication issue or an error response from the server.
   */
  public void resumeSourcePartitions(String datastreamName, StringMap sourcePartitions)
      throws RemoteInvocationException {
    Instant startTime = Instant.now();
    PollUtils.poll(() -> {
      try {
        ActionRequest<Void> request =
            _builders.actionResumeSourcePartitions().id(datastreamName).sourcePartitionsParam(sourcePartitions).build();
        ResponseFuture<Void> datastreamResponseFuture = _restClient.sendRequest(request);
        return datastreamResponseFuture.getResponse();
      } catch (RemoteInvocationException e) {
        if (ExceptionUtils.getRootCause(e) instanceof TimeoutException) {
          LOG.warn("Timeout: pauseSourcePartitions. May retry...", e);
          return null;
        }
        if (isNotFoundHttpStatus(e)) {
          LOG.warn(String.format("Datastream {%s} is not found", datastreamName), e);
          throw new DatastreamNotFoundException(datastreamName, e);
        } else {
          String errorMessage =
              String.format("Resume Datastream partitions failed with error. Datastream: {%s}, Partitions: {%s}",
                  datastreamName, sourcePartitions);
          ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        }
        return null; // not reachable
      }
    }, Objects::nonNull, getRetryPeriodMs(), getRetryTimeoutMs()).orElseThrow(RetriesExhaustedException::new);
    LOG.info("resumeSourcePartitions for datastream {}, sourcePartitions {} took {} ms", datastreamName, sourcePartitions,
        Duration.between(startTime, Instant.now()).toMillis());
  }
}
