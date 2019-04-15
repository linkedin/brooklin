/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.data.template.GetMode;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DatastreamUtils;


/**
 * Represents list of deduped datastreams.
 */
public class DatastreamGroup {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamGroup.class.getName());
  private final String _taskPrefix;
  private final List<Datastream> _datastreams;

  /**
   * Construct a datastream group
   * @param datastreams
   *  list of datastreams inside this group, the list cannot be null or empty
   */
  public DatastreamGroup(List<Datastream> datastreams) {
    Validate.notEmpty(datastreams, "datastreams cannot be null or empty.");
    Datastream ds = datastreams.get(0);
    _taskPrefix = DatastreamUtils.getTaskPrefix(datastreams.get(0));
    if (!datastreams.stream()
        .allMatch(d -> d.getConnectorName().equals(ds.getConnectorName()) && DatastreamUtils.getTaskPrefix(d)
            .equals(_taskPrefix))) {
      String msg =
          String.format("Datastreams within the group {%s} doesn't share the common connector name and task prefix",
              datastreams);
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    _datastreams = datastreams;
  }

  public String getTaskPrefix() {
    return _taskPrefix;
  }

  public List<Datastream> getDatastreams() {
    return _datastreams;
  }

  public String getConnectorName() {
    return _datastreams.get(0).getConnectorName();
  }

  /**
   * get the source partition of this DatastreamGroup, the source partition is the same for all
   * datastream inside this group
   */
  public Optional<Integer> getSourcePartitions() {
    return Optional.ofNullable(_datastreams.get(0).getSource()).map(x -> x.getPartitions(GetMode.NULL));
  }

  /**
   * determine if a DatastreamGroup is paused. A DatastreamGroup is considered pauses only if ALL the
   * datastreams in the group are paused.
   */
  public boolean isPaused() {
    boolean anyPaused = _datastreams.stream().anyMatch(ds -> ds.getStatus() == DatastreamStatus.PAUSED);
    boolean allPaused = _datastreams.stream().allMatch(ds -> ds.getStatus() == DatastreamStatus.PAUSED);
    if (anyPaused && !allPaused) {
      List<String> streamsWithStatus = _datastreams.stream()
          .filter(ds -> ds.getStatus() == DatastreamStatus.PAUSED)
          .map(ds -> ds.getName() + ds.getStatus())
          .collect(Collectors.toList());

      LOG.warn("Some datastreams are paused in a group, while others are not. " + "Datastreams: " + streamsWithStatus);
    }
    return allPaused;
  }

  /**
   *  determine if a DatastreamTask belongs to this group
   */
  public boolean belongsTo(DatastreamTask task) {
    return task.getTaskPrefix().equals(getTaskPrefix());
  }

  @Override
  public String toString() {
    String streamNames = _datastreams.stream().map(Datastream::getName).collect(Collectors.joining(","));
    return "DatastreamGroup{" + "taskPrefix='" + _taskPrefix + '\'' + ", datastreams=" + streamNames + '}';
  }
}
