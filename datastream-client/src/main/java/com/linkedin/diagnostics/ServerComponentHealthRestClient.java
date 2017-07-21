package com.linkedin.diagnostics;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.datastream.diagnostics.ServerComponentHealth;
import com.linkedin.datastream.server.diagnostics.DiagRequestBuilders;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.FindRequest;
import com.linkedin.restli.client.RestClient;

/**
 * Restli Client to call the ServerComponentHealth Find request to get the server status.
 */

public class ServerComponentHealthRestClient {
  private static final Logger LOG = LoggerFactory.getLogger(ServerComponentHealthRestClient.class);
  private final DiagRequestBuilders _builders;
  private final RestClient _restClient;

  public ServerComponentHealthRestClient(String dsmUri) {
    this(dsmUri, new TransportClientAdapter(new HttpClientFactory().getClient(Collections.<String, String>emptyMap())));
  }

  public ServerComponentHealthRestClient(String dmsUri, Map<String, String> httpConfig) {
    this(dmsUri, new TransportClientAdapter(new HttpClientFactory().getClient(httpConfig)));
  }

  public ServerComponentHealthRestClient(String dsmUri, Client r2Client) {
    Validate.notEmpty(dsmUri, "invalid DSM URI");
    Validate.notNull(r2Client, "null R2 client");
    dsmUri = StringUtils.appendIfMissing(dsmUri, "/");
    _builders = new DiagRequestBuilders();
    _restClient = new RestClient(r2Client, dsmUri);
  }

  public ServerComponentHealthRestClient(RestClient restClient) {
    Validate.notNull(restClient, "null restClient");
    _builders = new DiagRequestBuilders();
    _restClient = restClient;
  }

  /**
   * Get the ServerComponentHealth status from one server instance. This method makes a FIND rest call
   * to the ServerComponentHealth management service which in turn fetches this status from the component.
   * @param type
   *    Type of the component such as connector.
   * @param scope
   *    Scope of the component such as Espresso and Kafka.
   * @param content
   *    Request content should be passed to the component.
   * @return
   *    List of ServerComponentHealth object corresponding to the component.
   */
  public ServerComponentHealth getStatus(String type, String scope, String content) {
    List<ServerComponentHealth> response = getServerComponentHealthStatues(type, scope, content);
    if (response != null && !response.isEmpty()) {
      return response.get(0);
    } else {
      LOG.error("ServerComponentHealth getStatus {%s} {%s} failed with empty response.", type, scope);
      return null;
    }
  }

  public List<ServerComponentHealth> getServerComponentHealthStatues(String type, String scope, String content) {
    try {
      FindRequest<ServerComponentHealth> request = _builders.findByStatus().typeParam(type).scopeParam(scope).contentParam(content).build();
      return _restClient.sendRequest(request).getResponse().getEntity().getElements();
    } catch (RemoteInvocationException e) {
      LOG.error("Get serverComponentHealthStatus {%s} {%s} failed with error.", type, scope);
      return null;
    }
  }

  /**
   * Get the ServerComponentHealth statuses from all server instances. This method makes a FIND rest call
   * to the ServerComponentHealth management service which in turn fetches this status from the component.
   * @param type
   *    Type of the component such as connector.
   * @param scope
   *    Scope of the component such as Espresso and Kafka.
   * @param content
   *    Request content should be passed to the component.
   * @return
   *    List of ServerComponentHealth object corresponding to the component.
   */
  public ServerComponentHealth getAllStatus(String type, String scope, String content) {
    List<ServerComponentHealth> response = getServerComponentHealthAllStatus(type, scope, content);
    if (response != null && !response.isEmpty()) {
      return response.get(0);
    } else {
      LOG.error("ServerComponentHealth getAllStatus {%s} {%s} failed with empty response.", type, scope);
      return null;
    }
  }

  public List<ServerComponentHealth> getServerComponentHealthAllStatus(String type, String scope, String content) {
    try {
      FindRequest<ServerComponentHealth> request = _builders.findByAllStatus().typeParam(type).scopeParam(scope).contentParam(content).build();
      return _restClient.sendRequest(request).getResponse().getEntity().getElements();
    } catch (RemoteInvocationException e) {
      LOG.error("Get serverComponentHealthAllStatus {%s} {%s} failed with error.", type, scope);
      return null;
    }
  }

  /**
   * Shutdown the ServerComponentHealthRestClient
   */
  public void shutdown() {
    _restClient.shutdown(new FutureCallback<>());
  }
}
