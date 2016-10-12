package com.linkedin.datastream;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamNotFoundException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.server.dms.BootstrapRequestBuilders;
import com.linkedin.datastream.server.dms.DatastreamRequestBuilders;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.ActionRequest;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.DeleteRequest;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.IdResponse;


/**
 * Datastream REST Client
 */
public class DatastreamRestClient {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamRestClient.class);
  private final DatastreamRequestBuilders _builders;
  private final RestClient _restClient;
  private final BootstrapRequestBuilders _bootstrapBuilders;
  private final HttpClientFactory _httpClient;

  public DatastreamRestClient(String dsmUri) {
    Validate.notEmpty(dsmUri, "invalid DSM URI");
    dsmUri = StringUtils.appendIfMissing(dsmUri, "/");
    _builders = new DatastreamRequestBuilders();
    _bootstrapBuilders = new BootstrapRequestBuilders();
    _httpClient = new HttpClientFactory();
    final Client r2Client = new TransportClientAdapter(_httpClient.getClient(Collections.<String, String>emptyMap()));
    _restClient = new RestClient(r2Client, dsmUri);
  }

  private Datastream doGetDatastream(String datastreamName) throws RemoteInvocationException {
    GetRequest<Datastream> request = _builders.get().id(datastreamName).build();
    ResponseFuture<Datastream> datastreamResponseFuture = _restClient.sendRequest(request);
    return datastreamResponseFuture.getResponse().getEntity();
  }

  private static boolean isNotFoundHttpStatus(RemoteInvocationException e) {
    return (e instanceof RestLiResponseException
        && ((RestLiResponseException) e).getStatus() == HttpStatus.S_404_NOT_FOUND.getCode());
  }

  /**
   * Get the complete datastream object corresponding to the datastream name. This method makes a GET rest call
   * to the Datastream management service which in turn fetches this Datastream object from the store (zookeeper).
   * @param datastreamName
   *    Name of the datastream that should be retrieved.
   * @return
   *    Datastream object corresponding to the datastream. This method will not return null.
   * @throws com.linkedin.datastream.common.DatastreamRuntimeException
   *    Throws DatastreamNotFoundException if the datastream doesn't exist,
   *    Throws DatastreamRuntimeException for any other errors encountered while fetching the datastream.
   *    If there are any other network/ system level errors while sending the request or receiving the response.
   */
  public Datastream getDatastream(String datastreamName) {
    try {
      return doGetDatastream(datastreamName);
    } catch (RemoteInvocationException e) {
      if (isNotFoundHttpStatus(e)) {
        LOG.warn(String.format("Datastream {%s} is not found", datastreamName), e);
        throw new DatastreamNotFoundException(datastreamName, e);
      } else {
        String errorMessage = String.format("Get Datastream {%s} failed with error.", datastreamName);
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
        return null;
      }
    }
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
   * @throws com.linkedin.datastream.common.DatastreamRuntimeException
   */
  public Datastream waitTillDatastreamIsInitialized(String datastreamName, long timeoutMs)
      throws InterruptedException {
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

  private List<Datastream> getAllDatastreams(GetAllRequest<Datastream> request) {
    ResponseFuture<CollectionResponse<Datastream>> datastreamResponseFuture = _restClient.sendRequest(request);
    try {
      return datastreamResponseFuture.getResponse().getEntity().getElements();
    } catch (RemoteInvocationException e) {
      String errorMessage = "Get All Datastreams failed with error.";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    }

    return null;
  }

  /**
   * Get all the datastream objects available on the sever. This method makes a GET rest call
   * to the Datastream management service which in turn fetches all the Datastream objects from the store (zookeeper).
   * Entries will be return in lexicographical based on their getName() property.
   *
   * @return all the Datastream objects
   * @for any errors encountered while fetching the datastream.
   */
  public List<Datastream> getAllDatastreams() {
    return getAllDatastreams(_builders.getAll().build());
  }

  /**
   * Get all the datastream objects available on the sever. This method makes a GET rest call
   * to the Datastream management service which in turn fetches all the Datastream objects from the store (zookeeper).
   * Entries will be return in lexicographical based on their getName() property.
   *
   * @param start index of the first datastream to produce
   * @param count maximum number of entries to be produced
   * @return
   * @throws DatastreamRuntimeException
   */
  public List<Datastream> getAllDatastreams(int start, int count) {
    return getAllDatastreams(_builders.getAll().paginate(start, count).build());
  }

  /**
   * Creates a new datastream. Name of the datastream must be unique. This method makes a POST rest call to the
   * Datastream management service which validates the datastream object and writes it to the store (zookeeper).
   * @param datastream
   *   Datastream that needs to be created.
   * @for any errors encountered while creating the datastream.
   * @throws com.linkedin.r2.RemoteInvocationException for any network/system level errors encountered
   *   while sending the request or receiving the response.
   */
  public void createDatastream(Datastream datastream) {
    CreateIdRequest<String, Datastream> request = _builders.create().input(datastream).build();
    ResponseFuture<IdResponse<String>> datastreamResponseFuture = _restClient.sendRequest(request);
    try {
      datastreamResponseFuture.getResponse();
    } catch (RemoteInvocationException e) {
      if (e instanceof RestLiResponseException && ((RestLiResponseException) e).getStatus() == HttpStatus.S_409_CONFLICT
          .getCode()) {
        String msg = String.format("Datastream %s already exists", datastream.getName());
        LOG.warn(msg, e);
        throw new DatastreamAlreadyExistsException(msg);
      } else {
        String errorMessage = String.format("Create Datastream {%s} failed with error.", datastream);
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
      }
    }
  }

  /**
   * Create the bootstrap datastream corresponding to the change capture datastream. This makes a REST call to the
   * Datastream management service. The datastream management service checks if there is an existing bootstrap datastream
   * that can be used, if so it returns the existing bootstrap datastream or it will create a new one and returns that
   * @param bootstrapDatastream
   *    BootstrapDatastream that needs to be created.
   * @return
   * @throws RemoteInvocationException
   *    If there are any other network/ system level errors while sending the request or receiving the response.
   * @throws DatastreamRuntimeException
   *    for any errors encountered while creating the bootstrap datastream.
   *
   */
  public Datastream createBootstrapDatastream(Datastream bootstrapDatastream) {
    ActionRequest<Datastream> request =
        _bootstrapBuilders.actionCreate().boostrapDatastreamParam(bootstrapDatastream).build();
    ResponseFuture<Datastream> datastreamResponseFuture = _restClient.sendRequest(request);
    try {
      return datastreamResponseFuture.getResponse().getEntity();
    } catch (RemoteInvocationException e) {
      if (e instanceof RestLiResponseException && ((RestLiResponseException) e).getStatus() == HttpStatus.S_409_CONFLICT
          .getCode()) {
        String msg = String.format("Datastream %s already exists", bootstrapDatastream.getName());
        LOG.warn(msg, e);
        throw new DatastreamAlreadyExistsException(msg);
      } else {
        String errorMessage = String.format("Create Bootstrap Datastream {%s} failed with error.", bootstrapDatastream);
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
      }
    }

    return null;
  }

  /**
   * Delete the datastream with the name. This method makes a DELETE rest call to the Datastream management service
   * on the DatastreamResource which in turn deletes the entity from the zookeeper. All the connectors that
   * are serving the datastream will get notified to stop producing events for the datastream.
   * @param datastreamName
   *   Name of the datastream that should be deleted.
   * @throws DatastreamRuntimeException
   *   When the datastream is not found or any other error happens on the server.
   */
  public void deleteDatastream(String datastreamName) {
    DeleteRequest<Datastream> request = _builders.delete().id(datastreamName).build();
    ResponseFuture<EmptyRecord> response = _restClient.sendRequest(request);
    try {
      response.getResponse();
    } catch (RemoteInvocationException e) {
      String errorMessage = String.format("Delete Datastream {%s} failed with error.", datastreamName);
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    }
  }

  /**
   * Check whether the specified datastream exists in the current Datastream cluster.
   * @param datastreamName name of the datastream to be checked
   * @return whether such datastream exists
   */
  public boolean datastreamExists(String datastreamName) {
    try {
      doGetDatastream(datastreamName);
      return true;
    } catch (RemoteInvocationException e) {
      if (isNotFoundHttpStatus(e)) {
        LOG.debug(String.format("Datastream {%s} is not found", datastreamName));
      } else {
        String errorMessage = String.format("Get Datastream {%s} failed with error.", datastreamName);
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
      }
    }
    return false;
  }

  /**
   * Shutdown the DatastreamRestClient
   */
  public void shutdown() {
    _restClient.shutdown(new FutureCallback<>());
    _httpClient.shutdown(new FutureCallback<>());
  }
}
