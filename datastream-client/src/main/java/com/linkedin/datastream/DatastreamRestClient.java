package com.linkedin.datastream;

import java.util.Collections;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamBuilders;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamNotFoundException;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.CreateRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.HttpStatus;


/**
 * Datastream REST Client
 */
public class DatastreamRestClient {

  private final DatastreamBuilders _builders;
  private final RestClient _restClient;

  public DatastreamRestClient(String dsmUri){
    _builders = new DatastreamBuilders();

    final HttpClientFactory http = new HttpClientFactory();
    final Client r2Client = new TransportClientAdapter(
        http.getClient(Collections.<String, String>emptyMap()));
    _restClient = new RestClient(r2Client, dsmUri);
  }

  /**
   * Get the complete datastream object corresponding to the datastream name. This method makes a GET rest call
   * to the Datastream management service which inturn fetches this Datastream object from the store (zookeeper).
   * @param datastreamName
   *    Name of the datastream that should be retrieved.
   * @return
   *    Datastream object corresponding to the datastream. This method will not return null.
   * @throws DatastreamException
   *    Throws DatastreamNotFoundException if the datastream doesn't exist,
   *    Throws DatastreamException for any other errors encountered while fetching the document schema.
   * @throws com.linkedin.r2.RemoteInvocationException
   *    If there are any other network/ system level errors while sending the request or receiving the response.
   */
  public Datastream getDatastream(String datastreamName) throws DatastreamException, RemoteInvocationException {
    GetRequest request = _builders.get().id(datastreamName).build();
    ResponseFuture<Datastream> datastreamResponseFuture = _restClient.sendRequest(request);
    Response<Datastream> response = datastreamResponseFuture.getResponse();

    if(response.hasError()) {
      if(response.getStatus() == HttpStatus.S_404_NOT_FOUND.getCode()) {
        throw new DatastreamNotFoundException(datastreamName);
      }
      throw new DatastreamException(String.format("Get Datastream {%s} failed with error %s", datastreamName,
          response.getError()));
    }
    return response.getEntity();
  }

  /**
   * Creates a new datastream. Name of the datastream must be unique.
   * @param datastream
   *   Datastream that needs to be created.
   * @throws DatastreamException for any errors encountered while creating the datastream.
   * @throws com.linkedin.r2.RemoteInvocationException for any network/system level errors encountered
   *   while sending the request or receiving the response.
   */
  public void createDatastream(Datastream datastream) throws DatastreamException, RemoteInvocationException {
    CreateRequest request = _builders.create().input(datastream).build();
    ResponseFuture<Datastream> datastreamResponseFuture = _restClient.sendRequest(request);
    Response<Datastream> response = datastreamResponseFuture.getResponse();
    if(response.hasError()) {
      throw new DatastreamException(String.format("Create Datastream {%s} failed with error %s", datastream,
          response.getError()));
    }
  }

  public Datastream createBootstrapDatastream() {
    return null;
  }
}
