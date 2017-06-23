package com.linkedin.datastream.server;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.data.template.GetMode;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamUtils;


/**
 * Represents list of de-duped datastreams.
 */
public class DatastreamGroup {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamGroup.class.getName());
  private final String _taskPrefix;
  private final List<Datastream> _datastreams;

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

  public Optional<Integer> getSourcePartitions() {
    return Optional.ofNullable(_datastreams.get(0).getSource()).map(x -> x.getPartitions(GetMode.NULL));
  }

  // A Datastream Group is paused, only if ALL the datastreams in the group are in paused.
  public boolean isPaused() {
    return _datastreams.stream().allMatch(ds -> ds.isPaused(GetMode.DEFAULT));
  }
}
