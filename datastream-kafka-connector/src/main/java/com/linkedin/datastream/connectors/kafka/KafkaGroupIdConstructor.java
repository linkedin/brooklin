package com.linkedin.datastream.connectors.kafka;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.server.DatastreamTask;

public class KafkaGroupIdConstructor implements GroupIdConstructor {

  private final boolean _isGroupIdHashingEnabled;
  private final String _clusterName;

  // TODO: Add cluster name to group ID after checking with Thomas/Justin
  // TODO: Blocker: would like to see if cluster name can be derived in some other way
  // TODO: rather making group ID dependent on it and calculations convoluted.

  public KafkaGroupIdConstructor(boolean isGroupIdHashingEnabled, String clusterName) {
    _isGroupIdHashingEnabled = isGroupIdHashingEnabled;
    _clusterName = clusterName;
  }

  @Override
  public String constructGroupId(Datastream datastream) {
    if (_isGroupIdHashingEnabled) {
      return constructGroupId(DatastreamUtils.getTaskPrefix(datastream), _clusterName);
    } else {
      return constructGroupId(KafkaConnectionString.valueOf(datastream.getSource().getConnectionString()),
          datastream.getDestination().getConnectionString());
    }
  }

  @Override
  public String constructGroupId(DatastreamTask task) {
    if (_isGroupIdHashingEnabled) {
      return constructGroupId(task.getTaskPrefix(), _clusterName);
    } else {
      return constructGroupId(KafkaConnectionString.valueOf(task.getDatastreamSource().getConnectionString()),
          task.getDatastreamDestination().getConnectionString());
    }
  }

  private String constructGroupId(KafkaConnectionString srcConnString, String dstConnString) {
    return srcConnString + "-to-" + dstConnString;
  }
}