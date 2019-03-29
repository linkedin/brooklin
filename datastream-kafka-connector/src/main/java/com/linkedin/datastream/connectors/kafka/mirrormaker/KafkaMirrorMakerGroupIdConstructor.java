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

/**
 * Implementation of {@link GroupIdConstructor} for kafka mirror maker connectors.
 * The class generates group ID for given datastream/task according to kafka mirrormaker connector conventions.
 */
public class KafkaMirrorMakerGroupIdConstructor implements GroupIdConstructor {
  private final String _clusterName;
  private boolean _isGroupIdHashingEnabled;

  /**
   * Constructor for KafkaMirrorMakerGroupIdConstructor
   * @param isGroupIdHashingEnabled Indicates if group ID generated should be hashed. In that case, cluster name is
   *                                appended to the hashed group ID to indicate origin of the group ID.
   * @param clusterName Name of the cluster where the group ID constructor is running. This should be the same as
   *                    cluster where corresponding datastream server is running. The cluster name is used in
   *                    generating group ID if isGroupIdHashingEnabled argument is set to true.
   */
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

