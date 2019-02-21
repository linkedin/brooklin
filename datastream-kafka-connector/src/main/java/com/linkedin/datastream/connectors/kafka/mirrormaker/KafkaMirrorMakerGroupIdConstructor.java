/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.server.DatastreamTask;


public class KafkaMirrorMakerGroupIdConstructor implements GroupIdConstructor {
  private boolean _isGroupIdHashingEnabled;
  private final String _clusterName;

  public KafkaMirrorMakerGroupIdConstructor(boolean isGroupIdHashingEnabled, String clusterName) {
    _isGroupIdHashingEnabled = isGroupIdHashingEnabled;
    _clusterName = clusterName;
  }

  @Override
  public String constructGroupId(Datastream datastream) {
    if (_isGroupIdHashingEnabled) {
      return constructGroupId(DatastreamUtils.getTaskPrefix(datastream), _clusterName);
    } else {
      return datastream.getName();
    }
  }

  @Override
  public String constructGroupId(DatastreamTask datastreamTask) {
    return constructGroupId(datastreamTask.getDatastreams().get(0));
  }
}

