/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.diagnostics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.diagnostics.position.ConnectorPositionReport;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.diagnostics.ConnectorPositionReportRestClient;
import com.linkedin.diagnostics.ConnectorPositionReportRestClientFactory;


/**
 * ConnectorPositionReportAggregator makes REST calls to all servers in the cluster to get their
 * {@link ConnectorPositionReport} data, and then aggregates them into a single {@link ConnectorPositionReport} result.
 */
public class ConnectorPositionReportAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectorPositionReportAggregator.class);

  /**
   * Number of threads to use when fetching data in parallel.
   */
  private static final int THREAD_POOLS_SIZE = 32;

  /**
   * The thread pool to use when fetching data.
   */
  private static final ForkJoinPool WORKER_POOL = new ForkJoinPool(THREAD_POOLS_SIZE, pool -> {
    final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
    worker.setName(ConnectorPositionReportAggregator.class.getSimpleName() + "-Task-" + worker.getPoolIndex());
    return worker;
  }, (t, e) -> LOG.error("Uncaught exception in thread {}", t, e), true);

  private final Coordinator _coordinator;
  private final boolean _useHttps;
  private int _restPort;
  private final String _restPath;
  private final Map<String, Object> _clientProperties;

  /**
   * Constructor for {@link ConnectorPositionReportAggregator}.
   *
   * @param coordinator the coordinator used by the cluster (used to determine which instances are in the cluster
   *                 currently)
   * @param useHttps if we should be using https
   * @param restPath the restPath to use when making the request (should point to the DMS endpoint)
   * @param restPort the port to connect to other instances with
   * @param clientProperties the client properties to use when constructing the underlying http clients
   */
  public ConnectorPositionReportAggregator(@NotNull final Coordinator coordinator, final boolean useHttps,
      @NotNull final String restPath, final int restPort, @NotNull final Map<String, Object> clientProperties) {
    _coordinator = coordinator;
    _useHttps = useHttps;
    _restPath = restPath.startsWith("/") ? restPath : "/" + restPath;
    _restPort = restPort;
    _clientProperties = clientProperties;
  }

  /**
   * Makes a REST call to get the {@link ConnectorPositionReport} data from each of the instances in the cluster
   * (in parallel) and aggregates their responses.
   *
   * @return a Map of instances to data returned by that instance
   */
  @NotNull
  public Map<String, Optional<ConnectorPositionReport>> getReports() {
    final Map<String, Optional<ConnectorPositionReport>> responses = new ConcurrentHashMap<>();

    // Get the list of instances and seed them in the map
    final List<String> instances = _coordinator.getLiveInstances();
    instances.forEach(instance -> responses.put(instance, Optional.empty()));

    // Fetch the reports from every instance
    try {
      WORKER_POOL.submit(() -> instances.parallelStream().forEach(instance -> {
        try {
          final String dmsUri = getDmsUri(instance);
          final ConnectorPositionReportRestClient restClient
              = ConnectorPositionReportRestClientFactory.getClient(dmsUri, _clientProperties);
          final ConnectorPositionReport report = restClient.getReport(false);
          responses.put(instance, Optional.ofNullable(report));
        } catch (Exception e) {
          LOG.error("Received exception from instance {} when fetching connector position report", instance, e);
        }
      })).get();
    } catch (InterruptedException e) {
      LOG.warn("Got interrupted exception when fetching ConnectorPositionReport objects from instances", e);
    } catch (ExecutionException e) {
      LOG.warn("Got execution exception when fetching ConnectorPositionReport objects from instances", e);
    }

    return responses;
  }

  /**
   * Gets the DMS uri that should be used to connect to the given instance.
   *
   * @param instanceName the instance name that the Brooklin server has joined the cluster with
   * @return the DMS uri to use to get data from that instance
   */
  @NotNull
  private String getDmsUri(@NotNull final String instanceName) {
    /* TODO: Add interface in Coordinator/ZkAdapter to get the hostname and port of a given instance, which would remove
        some of the assumptions that are needed here */

    // We assume that the instance name is in the format "{hostname}-{sequenceNumber}"
    final String hostName = instanceName.replaceAll("^(.*)-(\\d+)$", "$1");

    if (_useHttps) {
      return "https://" + hostName + ":" + _restPort + _restPath;
    }
    return "http://" + hostName + ":" + _restPort + _restPath;
  }

  /**
   * Set the port to be used to contact other instances on.
   */
  public void setPort(final int restPort) {
    if (_restPort == 0) {
      _restPort = restPort;
    }
  }
}