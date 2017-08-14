package com.linkedin.datastream.server.diagnostics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DiagnosticsAware;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.diagnostics.ServerComponentHealth;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.diagnostics.ServerComponentHealthRestClient;
import com.linkedin.diagnostics.ServerComponentHealthRestClientFactory;
import com.linkedin.r2.transport.http.client.HttpClientFactory;


/**
 * Server Component Health reader is to get the restli response from all  server instances, do the merge and return
 * the overall status of the server.
 */
public class ServerComponentHealthAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(ServerComponentHealthAggregator.class.getName());

  private static final String HTTP_REQUEST_TIMEOUT = "http.requestTimeout";
  private static final String HTTP_TIMEOUT = String.valueOf(Duration.ofSeconds(15).toMillis());

  private final ZkClient _zkClient;
  private final String _cluster;
  private int _restEndPointPort;
  private final String _restEndPointPath;

  public ServerComponentHealthAggregator(ZkClient zkClient, String cluster, int endPointPort, String endPointPath) {
    assert zkClient != null;
    assert cluster != null;

    _zkClient = zkClient;
    _cluster = cluster;
    _restEndPointPort = endPointPort;
    _restEndPointPath = endPointPath;
  }

  public List<ServerComponentHealth> getResponses(String componentType, String componentScope,
      String componentInputs, DiagnosticsAware component) {
    List<String> hosts = getLiveInstances();
    Map<String, String> responses = new ConcurrentHashMap<>();
    Map<String, String> errorResponses = new ConcurrentHashMap<>();

    hosts.parallelStream().forEach(hostName ->
    {
      ServerComponentHealthRestClient restClient = null;
      try {
        // Send requests to all the server live instances
        String dmsUri = getDmsUri(hostName);
        LOG.info("Send restli status request to " + dmsUri);
        restClient = ServerComponentHealthRestClientFactory.getClient(dmsUri, Collections.singletonMap(
            HttpClientFactory.HTTP_REQUEST_TIMEOUT, HTTP_TIMEOUT));

        ServerComponentHealth response = null;
        String errorMessage = "";
        try {
          response = restClient.getStatus(componentType, componentScope, componentInputs);
        } catch (Exception e) {
          errorMessage = "Received REST exception: " + e.toString() +  " from the host: " + dmsUri;
          LOG.error("Received REST exception from the host: {}", dmsUri, e);
        } finally {
          // No response received from a host, set error message
          if (response == null && errorMessage.isEmpty()) {
            errorMessage = "Failed to receive REST response from the host: " + dmsUri;
            LOG.error(errorMessage);
          }
          if (!errorMessage.isEmpty()) {
            errorResponses.put(hostName, errorMessage);
          } else {
            String message = "Received REST response from the host: " + dmsUri + " with status: " + response.getStatus();
            LOG.info(message);
            responses.put(hostName, response.getStatus());
          }
        }
      } finally {
        if (restClient != null) {
          restClient.shutdown();
        }
      }
    });

    ServerComponentHealth serverComponentHealth = new ServerComponentHealth();
    serverComponentHealth.setSucceeded(true);
    if (!errorResponses.isEmpty()) {
      serverComponentHealth.setSucceeded(false);
    }
    String localhostName = "UNKNOWN HOST";
    try {
      localhostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException uhe) {
      LOG.error("Could not get localhost Name", uhe.getMessage());
    }
    serverComponentHealth.setInstanceName(localhostName);
    serverComponentHealth.setErrorMessages(errorResponses.toString());
    try {
      String status = component.reduce(componentInputs, responses);
      serverComponentHealth.setStatus(status);
    } catch (Exception e) {
      serverComponentHealth.setSucceeded(false);
      serverComponentHealth.setStatus("");
    }

    return Arrays.asList(serverComponentHealth);
  }

  private List<String> getLiveInstances() {
    List<String> instances = new ArrayList<>();
    List<String> nodes = _zkClient.getChildren(KeyBuilder.liveInstances(_cluster));
    for (String node : nodes) {
      instances.add(_zkClient.readData(KeyBuilder.liveInstance(_cluster, node)));
    }
    return instances;
  }

  private String getDmsUri(String hostName) {
    String dmsUri = "";
    if (!hostName.startsWith("http")) {
      dmsUri += "http://";
    }
    if (hostName.split(":").length == 1) { // hostName does not include port number
      dmsUri += hostName + ":" + _restEndPointPort;
    } else {
      dmsUri += hostName;
    }
    if (!_restEndPointPath.isEmpty()) {
      dmsUri += "/" + _restEndPointPath;
    }
    return dmsUri;
  }

  public void setPort(int port) {
    if (_restEndPointPort == 0) {
      _restEndPointPort = port;
    }
  }

}
