package com.linkedin.datastream.connectors.kafka.mirrormaker;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaConnector;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.server.DatastreamTask;


public class KafkaMirrorMakerGroupIdConstructor implements GroupIdConstructor {
  private boolean _isGroupIdHashingEnabled;

  public KafkaMirrorMakerGroupIdConstructor(boolean isGroupIdHashingEnabled) {
    _isGroupIdHashingEnabled = isGroupIdHashingEnabled;
  }

  @Override
  public String constructGroupId(Datastream datastream) {
    String groupId = datastream.getName();
    return _isGroupIdHashingEnabled ? AbstractKafkaConnector.hashGroupId(groupId) : groupId;
  }

  @Override
  public String constructGroupId(DatastreamTask datastreamTask) {
    return constructGroupId(datastreamTask.getDatastreams().get(0));
  }

}

