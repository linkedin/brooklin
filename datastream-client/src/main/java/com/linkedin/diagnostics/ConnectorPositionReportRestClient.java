/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.diagnostics;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.diagnostics.position.ConnectorPositionReport;
import com.linkedin.datastream.server.diagnostics.ConnectorPositionsRequestBuilders;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;

/**
 * Rest.li client which interacts with the the /connectorPositions endpoint to get a ConnectorPositionReport.
 */
public class ConnectorPositionReportRestClient {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectorPositionReportRestClient.class);
  private static final ConnectorPositionsRequestBuilders REQUEST_BUILDERS = new ConnectorPositionsRequestBuilders();

  private final RestClient _restClient;

  /**
   * Constructs an instance of ConnectorPositionReportRestClient.
   *
   * @param restClient The rest.li client to use for sending requests
   */
  public ConnectorPositionReportRestClient(@NotNull final RestClient restClient) {
    _restClient = restClient;
  }

  /**
   * Returns the current position data for connectors running on this server (or every server in the cluster). See
   * {@link ConnectorPositionReport} for the object's contents.
   *
   * @param aggregate true if position data should be returned from every server, and false if it should just be
   *                  returned from the server who received the request
   */
  @Nullable
  public ConnectorPositionReport getReport(boolean aggregate) {
    try {
      final GetRequest<ConnectorPositionReport> request = REQUEST_BUILDERS.get().aggregateParam(aggregate).build();
      ResponseFuture<ConnectorPositionReport> responseFuture = _restClient.sendRequest(request);
      return responseFuture.getResponse().getEntity();
    } catch (RemoteInvocationException e) {
      LOG.error("Get report with aggregate = {} failed with an error", aggregate, e);
      return null;
    }
  }

}