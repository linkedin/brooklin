package com.linkedin.datastream.connectors.kafka;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.DatastreamTask;

public class KafkaGroupIdConstructor implements GroupIdConstructor {

  private boolean _isGroupIdHashingEnabled;

  // TODO: Add cluster name to group ID after checking with Thomas/Justin
  // TODO: Blocker: would like to see if cluster name can be derived in some other way
  // TODO: rather making group ID dependent on it and calculations convoluted.

  public KafkaGroupIdConstructor(boolean isGroupIdHashingEnabled) {
    _isGroupIdHashingEnabled = isGroupIdHashingEnabled;
  }

  @Override
  public String constructGroupId(Datastream datastream) {
    return constructGroupId(KafkaConnectionString.valueOf(datastream.getSource().getConnectionString()),
        datastream.getDestination().getConnectionString());
  }

  @Override
  public String constructGroupId(DatastreamTask task) {
    return constructGroupId(KafkaConnectionString.valueOf(task.getDatastreamSource().getConnectionString()),
        task.getDatastreamDestination().getConnectionString());
  }

  private String constructGroupId(KafkaConnectionString srcConnString, String dstConnString) {
    String groupId = srcConnString + "-to-" + dstConnString;
    return _isGroupIdHashingEnabled ? AbstractKafkaConnector.hashGroupId(groupId) : groupId;
  }
}