/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.diagnostics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DiagnosticsAware;
import com.linkedin.datastream.diagnostics.ServerComponentHealth;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.ErrorLogger;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTemplate;


/**
 * Resources classes are used by rest.li to process corresponding http request.
 * Note that rest.li will instantiate an object each time it processes a request.
 * So do make it thread-safe when implementing the resources.
 *
 * The format of the Rest.li request for the health status of all server instance
 * /diag?q=allStatus&type=connector&scope=espresso&content=componentParameters
 * where type and scope are used by the framework to decide which component to send the request,
 * and content is the parameter passed to the component which should implement the DiagnosticsAware interface.
 *
 * There is an extra Rest.li call to get the status of a single server:
 * /diag?q=status&type=connector&scope=espresso&content=componentParameters
 * It is not intended to be exposed to other teams such as Espresso, but it can be used internally for testing purpose.
 */
@RestLiCollection(name = "diag", namespace = "com.linkedin.datastream.server.diagnostics")
public class ServerComponentHealthResources extends CollectionResourceTemplate<String, ServerComponentHealth> {

  public static final String CONNECTOR_NAME = "connector";
  private static final Logger LOG = LoggerFactory.getLogger(ServerComponentHealthResources.class);
  private final ServerComponentHealthAggregator _aggregator;
  private final DatastreamServer _server;
  private final Coordinator _coordinator;
  private final ErrorLogger _errorLogger;

  /**
   * Constructor for ServerComponentHealthResources
   */
  public ServerComponentHealthResources(DatastreamServer datastreamServer) {
    _aggregator = datastreamServer.getServerComponentHealthAggregator();
    _server = datastreamServer;
    _coordinator = datastreamServer.getCoordinator();
    _errorLogger = new ErrorLogger(LOG, _coordinator.getInstanceName());
  }

  /**
   * Finder Request to get the status of all server instances.
   * You can access this FINDER method via /diag?q=status&type=connector&scope=espresso&content=...
   */
  @Finder("allStatus")
  public List<ServerComponentHealth> getAllStatus(@PagingContextParam PagingContext context,
      @QueryParam("type") String componentType,  // Connector or Transport provider
      @QueryParam("scope") String componentScope,  // Espresso, EspressoBootstrap and etc.
      @QueryParam("content") @Optional String componentInputs) {

    long startTime = System.currentTimeMillis();
    LOG.info("Restli getAllStatus request with name: {}, type: {} and content: {}.", componentType, componentScope,
        componentInputs);
    DiagnosticsAware component = getComponent(componentType, componentScope);
    if (component != null) {
      List<ServerComponentHealth> result = _aggregator.getResponses(componentType, componentScope, componentInputs, component);
      long endTime = System.currentTimeMillis();
      LOG.info("Processing request for getting status of server instances took {}ms", endTime - startTime);
      return result;
    } else {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Unknown component name and type");
      return Collections.emptyList();
    }
  }

  /**
   * Finder Request to get the status from one server instance.
   * You can access this FINDER method via /diag?q=stat&type=connector&scope=espresso&content=...
   */
  @Finder("status")
  public List<ServerComponentHealth> getStatus(@PagingContextParam PagingContext context,
      @QueryParam("type") String componentType,
      @QueryParam("scope") String componentScope,
      @QueryParam("content") @Optional String componentInputs) {

    long startTime = System.currentTimeMillis();
    LOG.info("Restli getStatus request with name: {}, type: {} and content: {}.", componentType, componentScope,
        componentInputs);

    ServerComponentHealth serverComponentHealth = new ServerComponentHealth();

    DiagnosticsAware component = getComponent(componentType, componentScope);
    if (component != null) {
      try {
        String status = component.process(componentInputs);
        serverComponentHealth.setStatus(status);
        serverComponentHealth.setSucceeded(true);
        serverComponentHealth.setErrorMessages("");
      } catch (Exception e) {
        serverComponentHealth.setStatus("");
        serverComponentHealth.setSucceeded(false);
        serverComponentHealth.setErrorMessages(e.toString());
      }

      String localhostName = "";
      try {
        localhostName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException uhe) {
        LOG.error("Could not get localhost Name {}", uhe.getMessage());
      }
      serverComponentHealth.setInstanceName(localhostName);
      long endTime = System.currentTimeMillis();
      List<ServerComponentHealth> result = Collections.singletonList(serverComponentHealth);
      LOG.info("Processing request for getting status of server instance took {}ms", endTime - startTime);
      return result;
    } else {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Unknown component name and type");
      return Collections.emptyList();
    }
  }

  // Get the component object
  private DiagnosticsAware getComponent(String componentType, String componentScope) {
    String componentTypeLowercase = componentType.toLowerCase();
    if (componentTypeLowercase.equals(CONNECTOR_NAME)) {
      Connector connector = _coordinator.getConnector(componentScope);
      if (connector instanceof DiagnosticsAware) {
        return (DiagnosticsAware) connector;
      }
    }
    return null;
  }
}
