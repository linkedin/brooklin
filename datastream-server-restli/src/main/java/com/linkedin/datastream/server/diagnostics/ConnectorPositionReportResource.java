/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.diagnostics;

import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.data.template.StringArray;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.diag.BrooklinInstanceInfo;
import com.linkedin.datastream.common.diag.PositionDataStore;
import com.linkedin.datastream.common.diag.PositionKey;
import com.linkedin.datastream.common.diag.PositionValue;
import com.linkedin.datastream.diagnostics.position.ConnectorPositionReport;
import com.linkedin.datastream.diagnostics.position.InstancePositionReport;
import com.linkedin.datastream.diagnostics.position.InstancePositionReportArray;
import com.linkedin.datastream.diagnostics.position.PositionData;
import com.linkedin.datastream.diagnostics.position.PositionDataArray;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiSimpleResource;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.SimpleResourceTemplate;


/**
 * Resource class used by Rest.li to process HTTP requests to the /connectorPositions endpoint.
 *
 * The format of the request to get the Connector position data for this server instance is:
 *   GET /connectorPositions
 * The format of the request to get the Connector position data for all server instances is:
 *   GET /connectorPositions?aggregate=true
 *
 * The data returned will be a {@link ConnectorPositionReport} object.
 */
@RestLiSimpleResource(name = "connectorPositions", namespace = "com.linkedin.datastream.server.diagnostics")
public class ConnectorPositionReportResource extends SimpleResourceTemplate<ConnectorPositionReport> {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectorPositionReportResource.class);

  /**
   * The aggregator used when aggregating responses from all server instances.
   */
  private final ConnectorPositionReportAggregator _reportAggregator;

  /**
   * Constructor for this resource.
   *
   * @param datastreamServer the DatastreamServer instance
   */
  public ConnectorPositionReportResource(@NotNull final DatastreamServer datastreamServer) {
    _reportAggregator = datastreamServer.getConnectorPositionReportAggregator();
  }

  /**
   * Implements GET /connectorPositions with the boolean query parameter 'aggregate'.
   *
   * This method handles returning responses for the following calls:
   *   GET /connectorPositions
   *   GET /connectorPositions?aggregate=false
   *   GET /connectorPositions?aggregate=true
   *
   * @param aggregate If true, the returned data will be aggregated data from every instance in the cluster. If false,
   *                  the returned data will only be from this instance.
   * @return Connector position data in a {@link ConnectorPositionReport} object
   */
  @RestMethod.Get
  public ConnectorPositionReport get(@QueryParam("aggregate") @Optional("false") boolean aggregate) {
    return aggregate ? getAllReports() : getMyReport();
  }

  /**
   * Queries the Connector position data from just our instance as stored in the globally-instantiated
   * {@link PositionDataStore} and then returns that data in a {@link ConnectorPositionReport} object.
   *
   * @return the Connector position data from just our instance
   */
  private ConnectorPositionReport getMyReport() {
    final ConnectorPositionReport report = getInitializedReport();

    // Create our report
    final InstancePositionReport instanceReport = new InstancePositionReport();
    report.getPositionReports().add(instanceReport);
    instanceReport.setInstance(report.getRespondingInstance());
    instanceReport.setPositions(new PositionDataArray());

    // Fetch our report data
    try {
      for (final String taskPrefix : PositionDataStore.getInstance().keySet()) {
        final Map<PositionKey, PositionValue> positions = PositionDataStore.getInstance().get(taskPrefix);
        if (positions != null) {
          for (final PositionKey key : positions.keySet()) {
            final PositionValue value = positions.get(key);
            if (value != null) {
              final PositionData position = new PositionData()
                  .setTaskPrefix(taskPrefix)
                  .setKey(JsonUtils.toJson(key))
                  .setValue(JsonUtils.toJson(value));
              instanceReport.getPositions().add(position);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Could not serialize position data", e);
      report.getErrorMessages().add("Could not serialize position data: " + e.getMessage());
      instanceReport.setPositions(new PositionDataArray()); // Don't allow bad/incomplete data to be in the result
    }

    // Set success information
    final boolean succeeded = report.getErrorMessages().isEmpty();
    report.setSucceeded(succeeded);
    if (!succeeded) {
      report.getFailedInstances().add(report.getRespondingInstance());
    }

    return report;
  }

  /**
   * Queries the Connector position data from every instance via {@link ConnectorPositionReportAggregator} and then puts
   * that data into a {@link ConnectorPositionReport} object.
   *
   * @return the Connector position data from all instances in the cluster
   */
  private ConnectorPositionReport getAllReports() {
    final ConnectorPositionReport report = getInitializedReport();

    // Fetch all reports
    try {
      _reportAggregator.getReports().forEach((instance, instanceReport) -> {
        if (!instanceReport.isPresent()) {
          report.getFailedInstances().add(instance);
          report.getErrorMessages().add("Could not get report from instance " + instance);
        }
        instanceReport.ifPresent(r -> report.getPositionReports().addAll(r.getPositionReports()));
      });
    } catch (Exception e) {
      LOG.warn("Error when fetching reports from other instances", e);
      report.getFailedInstances().add(report.getRespondingInstance());
      report.getErrorMessages().add("Error when fetching reports from other instances: " + e.getMessage());
    }

    // Mark success/failure
    report.setSucceeded(report.getErrorMessages().isEmpty());

    return report;
  }

  /**
   * Constructs and initializes a {@link ConnectorPositionReport} object.
   *
   * @return an initialized {@link ConnectorPositionReport} object
   */
  private ConnectorPositionReport getInitializedReport() {
    return new ConnectorPositionReport()
        .setFailedInstances(new StringArray())
        .setErrorMessages(new StringArray())
        .setPositionReports(new InstancePositionReportArray())
        .setRespondingInstance(BrooklinInstanceInfo.getInstanceName());
  }
}