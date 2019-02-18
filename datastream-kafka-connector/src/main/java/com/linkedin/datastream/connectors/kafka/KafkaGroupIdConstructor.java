/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.server.DatastreamTask;

public class KafkaGroupIdConstructor implements GroupIdConstructor {

  private final boolean _isGroupIdHashingEnabled;
  private final String _clusterName;

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